package Spark1.Chapter_8Eight

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val sql = new SQLContext(sc)
    val df1 = sql.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("D://公司//京天利//实习//数据分析与报告//多指标//stud.csv")


    df1.show()
    df1.describe("ORDER_ID", "DATE", "STUDY_TIME").show

  }
}