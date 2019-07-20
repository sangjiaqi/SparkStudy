package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/*
* @TF-IDF
* @词重要性统计
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

//  创建数据
    val senntenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

//  创建分词器
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordsData = tokenizer.transform(senntenceData)

//  创建向量化对象
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)

//  创建TF-IDF对象
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("label", "features").show()

    rescaledData.select("features").collect.foreach(println)

  }
}