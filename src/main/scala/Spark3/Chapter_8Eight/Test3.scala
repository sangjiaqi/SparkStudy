package Spark3.Chapter_8Eight

import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.sql.SparkSession

/*
* @Bisecting k-means
* @快速K均值聚类
* @sang
 */

object Test3 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

    //  读取数据集
    val dataset = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//kmeans//treaningDic//kmeans_data.txt")

    // 建立模型
    val bkm = new BisectingKMeans()
      .setK(2)
      .setSeed(1)

//  训练模型
    val model = bkm.fit(dataset)

    // 模型测试
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // 模型结果
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)

  }
}