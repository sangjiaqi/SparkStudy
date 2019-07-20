package Spark1.Chapter_8Eight

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/*
协同过滤算法
sang
 */
object Test5 {
  def main(args: Array[String]): Unit = {
    val sc = sparkContext("Test5", "local[*]")

    val data = sc.textFile("E://BaiduYunDownload//大数据//SparkLearning-master//file//data//mllib//input//test.data")
    val ratings = data.map(_.split(",") match {case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    ratings.collect().foreach(println)

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    val usersProducts = ratings.map{case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map{case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val ratesAndPreds = ratings.map{case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map{case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    println("Mean Squared Error = " + MSE)

  }

  def sparkContext(name: String, master: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(name).setMaster(master)
    val sc = new SparkContext(sparkConf)
    return sc
  }
}