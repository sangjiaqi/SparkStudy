package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/*
* @Normalizer
* @正则化，L1，L2， LInf
* @sang
 */

object Test16 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test16")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

//  创建normalizer
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)

    println("Normalized using L^1 norm")
    l1NormData.show()

    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)

    println("Normalized using L^Inf norm")
    lInfNormData.show()

  }
}