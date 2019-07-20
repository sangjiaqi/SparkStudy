package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/*
* @ChiSqSelector
* @卡方选择器
* @sang
 */

object Test27 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test27")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )
    val df = spark.createDataFrame(data).toDF("id", "features", "clicked")

//  创建选择权
    val selector = new ChiSqSelector()
      .setNumTopFeatures(2)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show()




  }
}