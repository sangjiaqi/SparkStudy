package Spark1.Chapter_8Eight

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
决策树、随机森林、GBDT模型
 */
object Test7 {
  def main(args: Array[String]): Unit = {

//  0构建spark对象
    val sc = sparkContext("Test7", "local[*]")
    val sqlContext = new SQLContext(sc)

//  1训练样本读取
    val data = sqlContext.read.format("libsvm").load("E://BaiduYunDownload//大数据//SparkLearning-master//file//data//mllib//input//classification//sample_libsvm_data.txt")
    data.show()

//  2标签进行索引编号
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

//  3样本划分
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

//  4训练决策树
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

//  4训练随机森林
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

//  4训练GBDT模型
    val gbdt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)



//  5将索引的标签转回原始标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

//  6构建Pipeline
    val pipeline1 = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val pipeline2 = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val pipeline3 = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbdt, labelConverter))

//  7Pipeline开始训练
    val model1: PipelineModel = pipeline1.fit(trainingData)

    val model2: PipelineModel = pipeline2.fit(trainingData)

    val model3: PipelineModel = pipeline3.fit(trainingData)

//  8测试结果
    testModel(model1, testData)

    testModel(model2, testData)

    testModel(model3, testData)

  }

  def sparkContext(name: String, master: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(name).setMaster(master)
    val sc = new SparkContext(sparkConf)
    return sc
  }

  def testModel(model: PipelineModel, testData: DataFrame): Unit = {
//  测试结果
    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(5)

//  各项指标
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator2.evaluate(predictions)
    println("f1 = " + f1)

    val evaluator3 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val Precision = evaluator3.evaluate(predictions)
    println("Presision = " + Precision)

    val evaluator4 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val Recall = evaluator4.evaluate(predictions)
    println("Recall = " + Recall)

    val evaluator5 = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val AUC = evaluator5.evaluate(predictions)
    println("Test AUC = " + AUC)

    val evaluator6 = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderPR")
    val aupr = evaluator6.evaluate(predictions)
    println("Test aupr = " + aupr)

  }
}