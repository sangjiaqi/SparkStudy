package Spark3.Chapter_7Sseven

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession

/*
* @Random Forest Regression
* @随机森林回归
* @sang
 */

object Test4 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test4")
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

    //  划分数据集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    //  创建模型
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    //  创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

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
    println(s"Root Mean Squared Error (RMSE) on text data = $rmse")

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression tree model:\n ${rfModel.toDebugString}")

  }
}