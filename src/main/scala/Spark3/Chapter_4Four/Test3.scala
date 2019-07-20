package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/*
* @CountVectorizer
* @词计数
* @sanng
 */

object Test3 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

//  创建countVectorizer
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("fetures")
      .setVocabSize(3)
      .setMinDF(2)

    val model = cv.fit(df).transform(df)

    model.show()



  }
}