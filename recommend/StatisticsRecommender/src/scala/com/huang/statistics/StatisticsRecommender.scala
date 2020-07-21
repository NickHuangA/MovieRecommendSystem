package com.huang.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StatisticsRecommender {

  case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                   shoot: String, language: String, genres: String, actors: String, directors: String)
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
  case class MongoConfig(uri:String, db:String)
  //这个是基准的推荐类
  case class Recommendation(mid:Int, score:Double)
  //根据标签选出评分最佳的10个电影
  case class GenresRecommendation(genres:String, recs:Seq[Recommendation])

  val MOST_SCORE_OF_NUMBER = 10
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.216.101:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建 SparkConf 配置
    val sparkConf = new
        SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加入隐式转换
    import spark.implicits._

    //将数据加载进来,转化为DF
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    // 1) 统计热门电影，历史评分最多的人
    val ratingMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    //写入数据库
    ratingMoviesDF
        .write
        .option("uri", mongoConfig.uri)
        .option("collection", RATE_MORE_MOVIES)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    // 2) 统计最近热门电影
    //注册一个UDF
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")

    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid")

    rateMoreRecentlyMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3) 统计电影的平均分数
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")

    averageMoviesDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 4) 统计每个标签最热门的电影
    //使用电影和，平均的得分inner join
    // 生成 movie avg
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))
    //电影的所有标签
    val genres =
      List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign",
        "History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    //movieWithScore 与 标签进行笛卡尔积
    val genresRDD = spark.sparkContext.makeRDD(genres)




    //什么时候RDD有row？？
    val genresTopMovies  = genresRDD.cartesian(movieWithScore.rdd)
        .filter{
          case(genres, row) =>
            row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
        }
        .map{
          case(genres, row) =>
            (genres, ((row.getAs[Int]("mid")), row.getAs[Double]("avg")))
        }.groupByKey()
        .map{
          case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2)
            .take(MOST_SCORE_OF_NUMBER).map(item => Recommendation(item._1, item._2)))
        }.toDF()

    genresTopMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()






  }

}
