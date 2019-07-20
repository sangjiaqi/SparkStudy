package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/*
* @word2Vec
* 词向量化
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

//  创建数据
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

//  创建word2vsc对象
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(documentDF).transform(documentDF)

    model.show()

  }
}