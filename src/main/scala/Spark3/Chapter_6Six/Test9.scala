package Spark3.Chapter_6Six

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/*
* @Naive Bayes
* @朴素贝叶斯
* @sang
 */

object Test9 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test9")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//classification//sample_libsvm_data.txt")

//  划分数据集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

//  创建模型
    val nb = new NaiveBayes()

    val model = nb.fit(trainingData)

//  预测模型
    val predictions = model.transform(testData)

    predictions.show()

//  评估模型
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

  }
}