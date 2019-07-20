package Spark3.MyMachineLearn

import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

/*
* @自考不过数据读取、清洗、预处理、建模、调参、评估
* @sang
 */

object MyML2 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("MyML2")
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
      .load("D://自考不过学习行为.csv")

    //  数据预处理
    val data2 = data
      .withColumn("year", data("class_date").substr(1, 4).cast(DoubleType))
      .withColumn("month", data("class_date").substr(6, 2).cast(DoubleType))
      .withColumn("day", data("class_date").substr(9, 2).cast(DoubleType))
      .select("order_id", "year", "month", "day", "study_time", "playback_time")

    val data3 = data2
      .groupBy("order_id")
      .agg(
        ((max("year") - min("year")) * 365 + (max("month") - min("month")) * 30 + (max("day") - min("day"))).as("days"),
        count("order_id").as("freq"),
        sum("study_time").as("time1"),
        sum("playback_time").as("time2")
      )

    val data4 = data3
      .join(order, data3("order_id") === order("id"), "leftouter")
      .na.fill(value = "0", cols = Array("id"))
      .withColumn("cim", regexp_replace(col("id").substr(1, 1), "\\D", "1"))
      .select("cim", "days", "freq", "time1", "time2")

    //  数据转换
    val assembler = new VectorAssembler()
      .setInputCols(data4.columns.drop(1))
      .setOutputCol("features")

    val data5 = assembler.transform(data4)

    data5.cache()

    val labelIndexer = new StringIndexer()
      .setInputCol("cim")
      .setOutputCol("label")
      .fit(data5)

    //  划分训练集
    val Array(train, test) = data5.randomSplit(Array(0.8, 0.2))

    //  创建模型
    val gbdt = new GBTClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")

    //  索引转换
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

    //  创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, gbdt, labelConverter))

    //  创建参数列表
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbdt.maxBins, Array(16, 32, 64))
      .addGrid(gbdt.maxDepth, Array(3, 5, 7))
      .addGrid(gbdt.maxIter, Array(10, 50, 100))
      .build()

    //  创建调参方法
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
    val model = trainValidationSplit.fit(train)

    //  评估模型
    Evaluators(model.bestModel, test)

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