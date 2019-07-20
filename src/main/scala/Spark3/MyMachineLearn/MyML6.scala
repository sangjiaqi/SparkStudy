package Spark3.MyMachineLearn

import org.apache.spark.ml.{Model, Pipeline}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

/**
  * This program clean the data and make modle
  * @version 1.00
  * @author sangjiaqi
  */

object MyML6 {

  def main(args: Array[String]): Unit = {

    //    create spark object
    val spark = SparkSession
      .builder()
      .appName("MyML5")
      .master("local[*]")
      .getOrCreate()

    //    read the data
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\大学\\论文\\实培计划\\shipei_i.csv")

    //    clean the data
    val data2 = data
      .drop("name")
      .withColumn("pid", data.col("id").substr(1, 2).cast(DoubleType))
      .withColumn("cid", data.col("id").substr(3, 2).cast(DoubleType))
      .withColumn("rid", data.col("id").substr(5, 2).cast(DoubleType))
      .withColumn("year", data.col("id").substr(7, 4).cast(DoubleType))
      .withColumn("month", data.col("id").substr(11, 2).cast(DoubleType))
      .withColumn("day", data.col("id").substr(13, 2).cast(DoubleType))
      .drop("id")

    //    NLP处理
    //    分词
    val tokenizer = new Tokenizer()
      .setInputCol("occupation")
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

    //    词重要性转化
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val data3 = idf.fit(featured).transform(featured)

//    特征处理
  //    哑变量化
    val labelIndexer1 = new StringIndexer()
      .setInputCol("province")
      .setOutputCol("provinceIndex")

    val index1 = labelIndexer1.fit(data3).transform(data3)

    val labelIndexer2 = new StringIndexer()
      .setInputCol("city")
      .setOutputCol("cityIndex")

    val index2 = labelIndexer2.fit(index1).transform(index1)

  //    特征聚合
    val assembler = new VectorAssembler()
      .setInputCols(Array("sex", "age", "provinceIndex", "cityIndex", "coverage", "pid", "cid", "rid", "year", "month", "day", "features"))
      .setOutputCol("feature")

    val data4 = assembler.transform(index2)
      .select("feature", "claim")

//    划分训练集
    val Array(train, test) = data4.randomSplit(Array(0.7, 0.3))

    train.cache()

//    创建流水线组建
    //    哑变量化
    val labelIndexer = new StringIndexer()
      .setInputCol("claim")
      .setOutputCol("label")
      .fit(data4)

    //    过多特征处理
    val featureIndexer = new VectorIndexer()
      .setInputCol("feature")
      .setOutputCol("features")
      .setMaxCategories(20)
      .fit(data4)

//    创建模型
    val gbdt = new GBTClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

//    创建流水线
    val pipline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbdt, labelConverter))

//    创建参数列表
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbdt.maxBins, Array(128))
      .addGrid(gbdt.maxDepth, Array(3, 5, 7))
      .addGrid(gbdt.maxIter, Array(10, 50, 100))
      .build()

//    创建调参方法
    val cv = new CrossValidator()
      .setEstimator(pipline)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipline)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

//    训练模型
    val model1 = trainValidationSplit.fit(train)

//    val model2 = cv.fit(train)

//    评估模型
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