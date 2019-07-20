package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/*
* @Bucketizer
* @离散化重组
* @sang
 */

object Test20 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test20")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

//  创建bucketizer
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()



  }
}