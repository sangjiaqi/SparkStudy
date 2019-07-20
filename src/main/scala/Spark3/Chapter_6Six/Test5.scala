package Spark3.Chapter_6Six

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/*
* @Gradient-boosted tree classifier
* @梯度提升决策树
* @sang
 */

object Test5 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test5")
      .master("local[*]")
      .getOrCreate()

    //  读取数据集
    val data = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//classification//sample_libsvm_data.txt")

    //  数据转化
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    //  划分训练集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    //  创建模型
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    //  数据转化
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    //  创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    //  训练模型
    val model = pipeline.fit(trainingData)

    //  预测模型
    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "label", "features").show()

    //  模型评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification forest model:\n ${gbtModel.toDebugString}")

  }
}