package Spark3.Chapter_6Six

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/*
* @Linear Support Vector Machine
* @支持向量机
* @sang
 */

object Test7 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test7")
      .master("local[*]")
      .getOrCreate()

    //  读取数据集
    val data = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//classification//sample_libsvm_data.txt")

    //  创建模型
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

//  测试模型
    val lsvcModel = lsvc.fit(data)

    //  模型评估
    println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")

  }
}