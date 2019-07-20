package Spark3.Chapter_8Eight

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession

/*
* @K-means
* @K均值聚类
* @sang
 */

object Test1 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test1")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val dataset = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//kmeans//treaningDic//kmeans_data.txt")

//  创建模型
    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1L)

//  训练模型
    val model = kmeans.fit(dataset)

//  测试模型
    val predictions = model.transform(dataset)

//  评估模型
//    val evaluator = new ClusteringEvaluator()
//
//    val silhouette = evaluator.evaluate(predictions)
//    println(s"Silhouette with squared euclidean distance = $silhouette")
//
//  模型结果
//    println("Cluster Centers: ")
//    model.clusterCenters.foreach(println)

  }
}