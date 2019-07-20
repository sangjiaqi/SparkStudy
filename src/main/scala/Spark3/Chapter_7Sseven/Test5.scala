package Spark3.Chapter_7Sseven

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SparkSession

/*
* @Gradient-boosted tree regression
* @梯度提升回归树
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
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//regression//sample_linear_regression_data.txt")

//  数据转化
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

//  切分数据集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

//  创建GBDT模型
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxBins(10)

//  创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, gbt))

//  训练模型
    val model = pipeline.fit(trainingData)

//  测试模型
    val predictions = model.transform(testData)

    predictions.select("prediction", "label", "features").show()

//  评估模型
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Leanrned regression GBT model:\n ${gbtModel.toDebugString}")

  }
}