package Spark3.Chapter_6Six

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/*
* @binomaial Logistic Regression
* @二元逻辑回归
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
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//classification//sample_libsvm_data.txt")

//  创建lr模型
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

//  训练模型
    val lrModle = lr.fit(training)

    println(s"Coefficients: ${lrModle.coefficients} Intercept: ${lrModle.intercept}")

//  多显示族进行逻辑回归
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

//  训练模型
    val mlrModel = mlr.fit(training)

    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
    
  }
}