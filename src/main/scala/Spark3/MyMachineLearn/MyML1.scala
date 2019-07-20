package Spark3.MyMachineLearn

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/*
* @慕课学习行为数据
* @sang
 */

object MyML1 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("MyML1")
      .master("local[8]")
      .getOrCreate()

    //  读取数据集
    val data = spark
      .read
      .format("csv")
      .option("header", true)
      .option("inferSchema", "true")
      .load("D://大学//论文//徐昕_慕课学习行为//桑嘉琦//edx_21.csv")

    //  数据转换
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.drop(1))
      .setOutputCol("Features")

    val data2 = assembler.transform(data).select("certificate", "features")

    val labelIndexer = new StringIndexer()
      .setInputCol("certificate")
      .setOutputCol("label")
      .fit(data2)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("Features")
      .setMaxCategories(10)
      .fit(data2)

    //  划分训练集
    val Array(train, test) = data2.randomSplit(Array(0.8, 0.2))

    //  创建模型
    val gbdt = new GBTClassifier()
      .setFeaturesCol("Features")
      .setLabelCol("label")

    //  索引转化
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

    //  创建交叉验证模型
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    //  训练模型
    val model = cv.fit(train)

    //  测试模型
    val predictions = model.bestModel.transform(test)

    //  评估模型
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val evaluator2 = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

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