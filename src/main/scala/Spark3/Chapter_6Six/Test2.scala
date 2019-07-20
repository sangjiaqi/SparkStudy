package Spark3.Chapter_6Six

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/*
* @Multinomial logistic regression
* @多分类逻辑回归
* @sang
 */

object Test2 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test2")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val training = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_multiclass_classification_data.txt")

    training.printSchema()

//  创建模型
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

//  训练模型
    val lrModel = lr.fit(training)

    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
    println(s"Intercepts: \n${lrModel.interceptVector}")

  }
}