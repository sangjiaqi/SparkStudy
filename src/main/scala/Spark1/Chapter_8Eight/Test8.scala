package Spark1.Chapter_8Eight

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}

object Test8 {
  def main(args: Array[String]): Unit = {

//  0构建Spark对象
    val sc = sparkContext("Test8", "local[*]")
    val sqlContext = new SQLContext(sc)

//  1数据读取
    val df1 = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("C://Users//sangjiaqi//Desktop//软件//Scala//mllib//insurence.csv")

    val asswmbler = new VectorAssembler()
      .setInputCols(df1.columns.drop(1))
      .setOutputCol("features")
    val output = asswmbler.transform(df1)

    val df2 = output.select("cim", "features")

    df2.show()

//  2标签进行索引编号
    val labelIndexer = new StringIndexer()
      .setInputCol("cim")
      .setOutputCol("indexedLabel")
      .fit(df2)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(df2)

//  3样本划分
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))

//  4训练决策树
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

//  4训练随机森林
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

//  4训练GBDTmx
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

//  7开始训练模型
    val model1 = pipeline1.fit(trainingData)

    val model2 = pipeline2.fit(trainingData)

    val model3 = pipeline3.fit(trainingData)

//  8测试结果
    println("-----------------------------------")
    println("Decision Tree")
    testModel(model1, testData)

    println("-----------------------------------")
    println("Random Forest")
    testModel(model2, testData)

    println("-----------------------------------")
    println("Gradient Boosting Decision Tree")
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
    predictions.select("predictedLabel", "cim", "features").show(5)

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