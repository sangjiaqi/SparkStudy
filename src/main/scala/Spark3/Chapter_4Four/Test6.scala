package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{NGram, Tokenizer}
import org.apache.spark.sql.SparkSession

/*
* @n-gram
* @将单词转化为词组
* @sang
 */

object Test6 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

//  创建分词对象
    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    val tokenized = tokenizer.transform(sentenceData)

//  创建n-gram对象
    val ngram = new NGram()
      .setInputCol("words")
      .setOutputCol("ngrams")
      .setN(2)

    val ngramDataFrame = ngram.transform(tokenized)

    ngramDataFrame.show()


  }
}