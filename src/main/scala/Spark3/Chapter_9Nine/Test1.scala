package Spark3.Chapter_9Nine

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/*
* @Collaborative Filtering
* @协同过滤商品推荐
* @sang
 */

object Test1 {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
    .builder()
    .appName("Test1")
    .master("local[*]")
    .getOrCreate()

    //  格式化文件
    import spark.implicits._

    def parseRatting(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    val ratings = spark
      .read
      .textFile("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//als//sample_movielens_ratings.txt")
      .map(parseRatting)
      .toDF()

//  划分数据集
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

//  创建数据集
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

//  训练模型
    val model = als.fit(training)

//  测试模型
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

//  评估模型
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

  }
}