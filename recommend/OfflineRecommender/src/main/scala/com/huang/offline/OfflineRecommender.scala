package com.huang.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String,language: String, genres: String, actors: String, directors: String )
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)
// 标准推荐对象，mid,score
case class Recommendation(mid: Int, score:Double)
// 用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])
// 电影相似度（电影推荐）
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  // 推荐表的名称
  val USER_RECS = "UserRecs" //用于离线推荐
  val MOVIE_RECS = "MovieRecs" //用于实时推荐

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.216.101:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建 spark session
    val sparkConf = new
        SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //读取 mongoDB 中的业务数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()
    //获取用户的数据集RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()

    //电影数据集 RDD[Int]
    val movieRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //构建训练集数据集
    val trainDate = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, lambda) = (50, 5, 0.1)
    //调用ASL
    val model = ALS.train(trainDate, rank, iterations, lambda)
    //计算需要预测的矩阵
    val userMovies = userRDD.cartesian(movieRDD)
    //得到预测值  （uid, mid, score）
    val preRatings = model.predict(userMovies)

    //将预测值排名前10的推荐给用户
    val userRecs = preRatings
      .filter(x => x.rating > 0.6)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2)
        .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //计算电影的相似度矩阵，为以后的实时推荐做准备
    // 1）得到电影矩阵（n x k）
    val movieFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }
    // 2)自连接并且过滤
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        case (x, y) => x._1 != y._1
      }
      .map{
        case (x, y) =>
          val simScore = consinSim(x._2, y._2)
          (x._1, (y._1, simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map{
        case (mid, item) => MovieRecs(mid, item.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()
    // 3) 写入数据库
    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.close()

  }

  def consinSim(x: DoubleMatrix, y: DoubleMatrix):Double = {
    x.dot(y) / (x.norm2() * y.norm2())
  }


}
