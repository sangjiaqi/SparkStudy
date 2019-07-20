package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/*
* @DCT
* @离散余弦变换（用于图像处理）
* @sang
 */

object Test10 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test10")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

//  创建DCT对象
    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDF = dct.transform(df)
    dctDF.show()


  }
}