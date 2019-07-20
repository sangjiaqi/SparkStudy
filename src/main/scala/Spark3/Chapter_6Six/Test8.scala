package Spark3.Chapter_6Six

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/*
* @One-vs-Rest classifier
* @将多分类拆解为二分类以预测
* @sang
 */

object Test8 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

//  取数据集
    val data = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_multiclass_classification_data.txt")

//  划分数据集
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

//  创建模型
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    val ovr = new OneVsRest()
      .setClassifier(classifier)

//  训练模型
    val ovrModel = ovr.fit(train)

//  预测模型
    val predictions = ovrModel.transform(test)

//  模型评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

  }
}