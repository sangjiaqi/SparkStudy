package Spark3.Chapter_6Six

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/*
* @Multilayer perceptron classifier
* @多层感知机
* @sang
 */

object Test6 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val data = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_multiclass_classification_data.txt")

    data.show()

//  划分数据集
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

//  创建多层感知机模型
    val layers = Array[Int](4, 5, 4, 3)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(1000)

//  训练模型
    val model = trainer.fit(train)

//  预测模型
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")

//  模型评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

  }
}