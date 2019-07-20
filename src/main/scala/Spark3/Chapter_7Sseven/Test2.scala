package Spark3.Chapter_7Sseven

import org.apache.spark.ml.regression.{GeneralizedLinearRegression, LinearRegression}
import org.apache.spark.sql.{Row, SparkSession}

/*
* @Generalized linear regression
* @广义线性回归（Gaussian\Binomial\Poisson\Gamma\Tweedie）
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
    val training = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//regression//sample_linear_regression_data.txt")

    //  创建模型
    val glr = new GeneralizedLinearRegression()
        .setFamily("gaussian")
        .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    //  训练模型
    val model = glr.fit(training)

    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    //  评估模型
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()

  }
}