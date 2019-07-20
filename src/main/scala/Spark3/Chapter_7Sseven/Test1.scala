package Spark3.Chapter_7Sseven

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}

/*
* @Linear regression
* @线性回归
* @sang
 */

object Test1 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test1")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val training = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//regression//sample_linear_regression_data.txt")

//  创建模型
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

//  训练模型
    val lrModel = lr.fit(training)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//  评估模型
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")

    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

  }
}