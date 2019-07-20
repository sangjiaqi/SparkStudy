package Spark3.MyMachineLearn

import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.huaban.analysis.jieba.JiebaSegmenter

/*
* @保准牛雇主责任保险被保险人职业NLP多分类
* @sang
 */

object MyML3 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("MyML3")
      .master("local[*]")
      .getOrCreate()

    //  读取文件
    val data1 = spark
      .read
      .format("csv")
      .option("header", "false")
      .load("D://导出职业.csv")
      .withColumnRenamed("_c0", "occupation")

    //  特征处理
    //    分词
    val tokenizer = new Tokenizer()
      .setInputCol("occupation")
      .setOutputCol("words")

    val tokenized = tokenizer.transform(data1)

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

    val data2 = idf.fit(featured).transform(featured).select("occupation", "features")

    data2.show()

    //  创建聚类模型
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("label")
      .setK(5)

    val gmm = new GaussianMixture()
      .setFeaturesCol("features")
      .setPredictionCol("label")
      .setK(5)

    //  训练模型
    val model = gmm.fit(data2)

    //  测试模型
    val predictions = model.transform(data2)

    //  获取数据集
    val data3 = predictions.select("occupation", "features", "label")

    //  划分训练集
    val Array(train, test) = data3.randomSplit(Array(0.8, 0.2))

    //  创建分类模型
    val mlr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setFamily("multinomial")

    val rf = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val ovr1 = new OneVsRest()
      .setClassifier(rf)

    val gbdt = new GBTClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val ovr2 = new OneVsRest()
      .setClassifier(gbdt)

    //  创建参数列表
    val paramGrid1 = new ParamGridBuilder()
      .addGrid(mlr.maxIter, Array(10, 50))
      .addGrid(mlr.regParam, Array(0.3, 0.5))
      .addGrid(mlr.elasticNetParam, Array(0.3, 0.7))
      .build()

    val paramGrid2 = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(16, 32))
      .addGrid(rf.maxDepth, Array(3, 5))
      .addGrid(rf.numTrees, Array(10, 30))
      .build()

    val paramGrid3 = new ParamGridBuilder()
      .addGrid(gbdt.maxBins, Array(16, 32))
      .addGrid(gbdt.maxDepth, Array(3, 5))
      .addGrid(gbdt.maxIter, Array(10, 30))
      .build()

    //  创建交叉验证模型
    val cv1 = new CrossValidator()
      .setEstimator(mlr)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid1)
      .setNumFolds(2)

    val cv2 = new CrossValidator()
      .setEstimator(ovr1)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid2)
      .setNumFolds(2)

    val cv3 = new CrossValidator()
      .setEstimator(ovr2)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid3)
      .setNumFolds(2)

    val trainValidationSplit1 = new TrainValidationSplit()
      .setEstimator(mlr)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid1)
      .setTrainRatio(0.7)

    val trainValidationSplit2 = new TrainValidationSplit()
      .setEstimator(ovr1)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid2)
      .setTrainRatio(0.7)

    val trainValidationSplit3 = new TrainValidationSplit()
      .setEstimator(ovr2)
      .setEvaluator(new MulticlassClassificationEvaluator().setMetricName("f1"))
      .setEstimatorParamMaps(paramGrid3)
      .setTrainRatio(0.7)

    //  训练模型
    val model1 = trainValidationSplit1.fit(train)

    val model2 = trainValidationSplit2.fit(train)

    val model3 = trainValidationSplit3.fit(train)

    val model4 = cv1.fit(train)

    val model5 = cv2.fit(train)

    val model6 = cv3.fit(train)

    //  评估模型
    println("\nMultinomal Logistic Regression | Leave-One-Out Cross Validation")
    Evaluators(model1, test)

    println("\nRandom Forest | Leave-One-Out Cross Validation")
    Evaluators(model2, test)

    println("\nGradient Boosting Decision Tree | Leave-One-Out Cross Validation")
    Evaluators(model3, test)

    println("\nMultinomal Logistic Regression | k-fold Cross Validation")
    Evaluators(model4, test)

    println("\nRandom Forest | k-fold Cross Validation")
    Evaluators(model5, test)

    println("\nGradient Boosting Decision Tree | l-fold Cross Validation")
    Evaluators(model6, test)

  }

  def Evaluators(model: Model[_], test: Dataset[Row]): Unit = {

    val predictions = model.transform(test)

    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("prediction")
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

  }

}