package com.huang.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSrmse {
  def main(args: Array[String]): Unit = {

    val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://192.168.216.101:27017/recommender",
    "mongo.db" -> "recommender"
    )
    //创建 SparkConf
    val sparkConf = new
        SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
    //创建 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._
    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score)).cache()

    //分割数据集
    val split = ratingRDD.randomSplit(Array(0.8, 0.2))

    val trainRDD = split(0)
    val testingRDD = split(1)

    //输出最优的参数
    adjustALSParams(trainRDD, testingRDD)
  }

  def adjustALSParams(trainRDD:RDD[Rating], testingRDD:RDD[Rating]){
    val result = for(rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainRDD, rank, 5, lambda)
        val rmse = getRMSE(model, testingRDD)
        (rank, lambda, rmse)
      }
    println(result.minBy(_._3))
  }

  def getRMSE(model : MatrixFactorizationModel, testingRDD: RDD[Rating]): Double = {
    val userMovies = testingRDD.map(item => (item.user, item.product))
    val predictRating = model.predict(userMovies)
    val real = testingRDD.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    //计算RMSE
    sqrt(
      real.join(predict).map{
        case((uid, mid), (real, predict)) =>
          val err = real - predict
          err*err
      }.mean()
    )
  }

}
