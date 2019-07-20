package Spark1.Chapter_8Eight

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/*
k均值聚类
sang
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    val sc = sparkContext("Test6", "local[*]")

    val data = sc.textFile("E://BaiduYunDownload//大数据//SparkLearning-master//file//data//mllib//input//kmeans//treaningDic//kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble))).cache()

    val numClusters = 2
    val numIterations = 20

    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }

  def sparkContext(name: String, master: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(name).setMaster(master)
    val sc = new SparkContext(sparkConf)
    return sc
  }
}