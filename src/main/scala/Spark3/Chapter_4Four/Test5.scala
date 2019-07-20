package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/*
* @StopWOrdRemover
* @移除停用词
* @sang
 */

object Test5 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test5")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

//  创建stopwordsremover
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val removed = remover.transform(dataSet)

    removed.show()

  }
}