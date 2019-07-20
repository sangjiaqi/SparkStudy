package Spark3.Chapter_8Eight

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession

/*
* @Gaussian Mixture Model
* @混合高斯聚类
* @sang
 */

object Test4 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test4")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val dataset = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//kmeans//treaningDic//kmeans_data.txt")

//  创建模型
    val gmm = new GaussianMixture()
      .setK(2)

//  训练模型
    val model = gmm.fit(dataset)

//  模型结果
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n"
      )
    }

  }
}