package com.huang

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 连接助手对象
object ConnHelper extends Serializable{
  //连接Redis
  lazy val jedis = new Jedis("192.168.216.101")
  //连接MongoDB
  lazy val mongoClient =
    MongoClient(MongoClientURI("mongodb://192.168.216.101:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)
// 标准推荐
case class Recommendation(mid:Int, score:Double)
// 用户的推荐
case class StreamRecs(uid:Int, recs:Seq[Recommendation])
//电影的相似度
case class MovieRecs(mid:Int, recs:Seq[Recommendation])

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.216.101:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个 SparkConf 配置
    val sparkConf = new
        SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    //处理流数据
    val ssc = new StreamingContext(sc, Seconds(2))
    implicit val mongConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //将得到的相似度矩阵转化为Map[Int, Map[Int, Double]]
    //并将其广播
    val simMoviesMatrix = spark
      .read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        recs => (recs.mid, recs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()

    val simMoviesMatrixBroadcast = sc.broadcast(simMoviesMatrix)

    //创建kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.216.101:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))

    //收集到评分流,通过Flume手机
    //UID|MID|SCORE|TIMESTAMP|
    val ratingStream = kafkaStream.map{
      case msg =>
        var attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).split("#")(0).toInt)
    }



    ratingStream.foreachRDD{
      rdd => rdd.map{
        case (uid, mid, score, timestame) =>
          println(">>>>>>>>>>>>>>>>>>")
          //从redis中获取M次电影的评分
          val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

          //获取流中的电影最相似的K个电影,作为候选集
          val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadcast.value)

          //计算候选电影的优先级
          val streamRecs = computeMovieScores(simMoviesMatrixBroadcast.value, userRecentlyRatings, simMovies)

          //将数据保存到MongoDB
          saveRecsToMongoDB(uid, streamRecs)
      }.count()
    }

    ssc.start()
    ssc.awaitTermination()

  }

  import scala.collection.JavaConversions._

  /**
    *
    * @param num 获取评分个数
    * @param uid 谁的评分
    * @param jedis redis端口
    * @return
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis):Array[(Int, Double)] = {
    //从用户队列中获取num个评分
    jedis.lrange("uid:" + uid.toString, 0, num - 1).map{
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取最相似的K个电影，并过滤已经评分过得
    * @param num 相似电影数量
    * @param mid 当前电影ID
    * @param uid 当前用户ID
    * @param simMovies 电影相似度广播矩阵
    * @param mongConfig MongoDB配置
    * @return
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int,
                      simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongConfig: MongoConfig):Array[Int] = {

    //从广播变量中获取与当前电影最相似的电影转化为Array
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经看过的电影
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid)).toArray.map{
      item => item.get("mid").toString.toInt
    }
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2)
      .take(num).map(x => x._1)
  }

  def computeMovieScores(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                         userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]):Array[(Int, Double)] = {

    //用于保存最后的每一个待选电影和他的评分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个电影的增强因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每一个电影的衰减因子
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for(topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings){
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)

      if(simScore > 0.6){
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        }else {
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }

    }

    score.groupBy(_._1).map{
      case (mid, sims) =>
        (mid, sims.map(_._2).sum / sims.length +
          math.log10(increMap.getOrDefault(mid, 1)) - math.log10(decreMap.getOrDefault(mid, 1)))

    }.toArray.sortWith(_._2 > _._2)


  }

  /**
    * 用于找出用户评分的电影和候选电影的相似度
    *
    * @param simMovies 相似度矩阵
    * @param userRatingMovie 用户评分的电影
    * @param topSimMovie 候选电影
    * @return
    */
  def getMoviesSimScore(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                        userRatingMovie: Int, topSimMovie: Int): Double ={
    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])
                       (implicit mongConfig: MongoConfig): Unit ={
    val streaRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streaRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" ->
      streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }

}
