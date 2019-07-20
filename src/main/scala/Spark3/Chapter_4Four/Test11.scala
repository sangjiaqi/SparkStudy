package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/*
* @StringgIndexer
* @字符床转换为索引
* @sang
 */

object Test11 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test11")
      .master("local[*]")
      .getOrCreate()

    //  创建数据集
    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    //  创建StringIndexer对象
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)

    indexed.show()

  }
}