package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

/*
* @VectorIndexer  ???
* @向量-索引变化
* @sang
 */

object Test14 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test14")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    val data = spark
      .read.
      format("libsvm").
      load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_libsvm_data.txt")

//  创建vectoindexer
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexed = indexer.fit(data).transform(data)

    indexed.show()
    indexed.collect().foreach(println)

  }
}