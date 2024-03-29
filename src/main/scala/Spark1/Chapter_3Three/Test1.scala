package Spark1.Chapter_3Three

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "E://BaiduYunDownload//大数据//spark生态圈//day28//test1.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}