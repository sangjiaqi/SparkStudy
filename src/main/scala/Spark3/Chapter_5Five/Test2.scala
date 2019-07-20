package Spark3.Chapter_5Five

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

/*
* @Train-Validation Split
* @切分法划分训练集
* @sang
 */

object Test2 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test2")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val data = spark
      .read.
      format("libsvm").
      load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_libsvm_data.txt")

//  划分训练集与验证集
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

//  创建模型
    val lr = new LinearRegression()
      .setMaxIter(10)

//  创建参数列表
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

//  创建训练、测试调参模型
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

//  训练模型
    val model = trainValidationSplit.fit(training)

//  模型测试
    model.transform(test)
      .select("features", "label", "prediction")
      .show()

  }
}