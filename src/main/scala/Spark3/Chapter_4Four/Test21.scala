package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/*
* @ElementwiseProduct
* @元素乘积
* @sang
 */

object Test21 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val dataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

//  创建ElementwiseProduct
    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show()

  }
}