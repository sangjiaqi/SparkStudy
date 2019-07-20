package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession

/*
* @Tokenization
* @分词器
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
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

//  创建普通分词器
    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    val tokenized = tokenizer.transform(sentenceDataFrame)

    tokenized.show()

//  创建正则分词器
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)

    regexTokenized.show()

  }
}