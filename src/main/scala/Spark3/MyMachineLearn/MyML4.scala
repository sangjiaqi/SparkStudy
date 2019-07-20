package Spark3.MyMachineLearn

import org.apache.spark.ml.{Model, Pipeline}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

/*
* @自考不过带文本变量数据读取、清洗、预处理、建模、调参、评估
* @sang
 */

object MyML4 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("MyML4")
      .master("local[*]")
      .getOrCreate()

//  读取数据
    val order = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D://自考不过保单.csv")
      .withColumnRenamed("order_id", "id")

    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D://自考不过学习行为2.csv")

//  数据处理
    val data2 = data
      .withColumn("year", data("date").substr(1, 4).cast(DoubleType))
      .withColumn("month", data("date").substr(6, 2).cast(DoubleType))
      .withColumn("day", data("date").substr(9, 2).cast(DoubleType))
      .groupBy("order_id")
      .agg(
        ((max("year") - min("year")) * 365 + (max("month") - min("month")) * 30 + (max("day") - min("day"))).as("days"),
        count("order_id").as("freq"),
        sum("view").as("time1"),
        sum("playback").as("time2"),
        concat_ws("", collect_list("subject")).as("subjects")
      )
      .join(order, data("order_id") === order("id"), "left outer")
      .na.fill(value = "0", cols = Array("id"))
      .withColumn("cim", regexp_replace(col("id").substr(1, 1), "\\D", "1"))
      .select("cim", "days", "freq", "time1", "time2", "subjects")

//  数据转换
//    分词
    val tokenizer = new Tokenizer()
      .setInputCol("subjects")
      .setOutputCol("words")

    val tokenized = tokenizer.transform(data2)

//    停用词
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filter")

    val removed = remover.transform(tokenized)

//    向量化
    val hashingTF = new HashingTF()
      .setInputCol("filter")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val featured = hashingTF.transform(removed)

//    词重要性
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val data3 = idf.fit(featured).transform(featured)
      .select("cim", "days", "freq", "time1", "time2", "features")

//    格式处理
    val assembler = new VectorAssembler()
      .setInputCols(data3.columns.drop(1))
      .setOutputCol("feature")

    val data4 = assembler.transform(data3)

//  划分训练集
    val Array(train, test) = data4.randomSplit(Array(0.7, 0.3))

    train.cache()

//  创建流水线组建
//    哑变量化
    val labelIndexer = new StringIndexer()
      .setInputCol("cim")
      .setOutputCol("label")
      .fit(data4)

//    过多特征处理
    val featureIndexer = new VectorIndexer()
      .setInputCol("feature")
      .setOutputCol("featuress")
      .setMaxCategories(10)
      .fit(data4)

//  创建模型
    val gbdt = new GBTClassifier()
      .setFeaturesCol("featuress")
      .setLabelCol("label")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

//  创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbdt, labelConverter))

//  创建参数列表
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbdt.maxBins, Array(16, 32, 64))
      .addGrid(gbdt.maxDepth, Array(3, 5, 7))
      .addGrid(gbdt.maxIter, Array(10, 50, 100))
      .build()

//    创建调参方法
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

//  训练模型
    val model1 = trainValidationSplit.fit(train)

//    val model2 = trainValidationSplit.fit(train)

//  评估模型
    Evaluators(model1, test)

//    Evaluators(model2, test)

  }

  def Evaluators(model: Model[_], test: Dataset[Row]): Unit = {

    val predictions = model.transform(test)

    predictions.cache()

    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("label")

    val evaluator2 = new BinaryClassificationEvaluator()
      .setRawPredictionCol("prediction")
      .setLabelCol("label")

    val accuracy = evaluator
      .setMetricName("accuracy")
      .evaluate(predictions)
    println(s"The accuracy = $accuracy")

    val f1 = evaluator
      .setMetricName("f1")
      .evaluate(predictions)
    println(s"The f1 = $f1")

    val Precision = evaluator
      .setMetricName("weightedPrecision")
      .evaluate(predictions)
    println(s"The Precision = $Precision")

    val Recall = evaluator
      .setMetricName("weightedRecall")
      .evaluate(predictions)
    println(s"The Recall = $Recall")

    val AUC = evaluator2
      .setMetricName("areaUnderROC")
      .evaluate(predictions)
    println(s"The AUC = $AUC")

    val areaUnderPR = evaluator2
      .setMetricName("areaUnderPR")
      .evaluate(predictions)
    println(s"The areaUnderPR = $areaUnderPR")

  }

}