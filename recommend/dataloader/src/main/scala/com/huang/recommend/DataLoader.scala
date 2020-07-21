package com.huang.recommend

import java.net.InetAddress

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


//1.创建样例类
//2.数据预处理
//3.数据插入两个数据库

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)
case class Ratings(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

case class MongoConfig(uri:String, db:String)
case class ESConfig(httpHosts:String, transportHosts:String, index:String,
                    clustername:String)


object DataLoader {

  val MOVIE_DATA_PATH = "E:\\workFile\\spark\\code\\MovieRecommendSystem\\recommend\\dataloader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "E:\\workFile\\spark\\code\\MovieRecommendSystem\\recommend\\dataloader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "E:\\workFile\\spark\\code\\MovieRecommendSystem\\recommend\\dataloader\\src\\main\\resources\\tags.csv"

  //前三个实在MongoDB中的表名，最后一个实在ES中的表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    //设定配置信息
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop101:9200",
      "es.transportHosts" -> "hadoop101:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    //导入数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingsRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

    //将movieRDD转化为DataFrame
    val movieDF = movieRDD.map( item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,
          attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val ratingsDF = ratingsRDD.map( item => {
      val attr = item.split(",")
      Ratings(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagDF = tagRDD.map( item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    // 声明两个个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get)


    storeDataInMongoDB(movieDF, ratingsDF, tagDF)

    import org.apache.spark.sql.functions._
    //数据预处理，插入ES中
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|",collect_set($"tag"))
        .as("tags"))
      .select("mid","tags")

    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    storeDataINES(movieWithTagsDF)


    spark.close()


  }

  def storeDataInMongoDB(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)
                        (implicit  mongoConfig: MongoConfig ):Unit ={

    //新建一个MongoDB连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //表存在，删除，否者创建
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将数据写入数据库
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

  def storeDataINES(movieWithTagsDF: DataFrame)(implicit esConfig:ESConfig): Unit ={
    //新建一个配置
    val setting:Settings = Settings.builder()
        .put("cluster.name", esConfig.clustername).build()

    //新建一个ES客户端
    val esClient = new PreBuiltTransportClient(setting)
    //需要将 TransportHosts 添加到 esClient 中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) => {
        esClient.addTransportAddress(new
            InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }

    //清除原先保留的
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入到 ES 中
    movieWithTagsDF
      .write
      .option("es.nodes",esConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index+"/"+ES_MOVIE_INDEX)

  }

}
