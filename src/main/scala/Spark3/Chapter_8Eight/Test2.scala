package Spark3.Chapter_8Eight

import org.apache.spark.ml.clustering.{KMeans, LDA}
import org.apache.spark.sql.SparkSession

/*
* @Latent Dirichlet allocation
* @Latent Dirichlet allocation
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
    val dataset = spark
      .read
      .format("libsvm")
      .load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//kmeans//treaningDic//kmeans_data.txt")

    dataset.show()

    //创建LDA模型
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)

  }
}