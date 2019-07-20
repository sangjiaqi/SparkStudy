package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/*
* @Binarizer
* @数据二元化
* @sang
 */

object Test7 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test7")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

//  创建binarizer对象
    val binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()

  }
}