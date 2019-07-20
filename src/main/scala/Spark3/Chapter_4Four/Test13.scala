package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.SparkSession

/*
* @OneHotEncoder
* @独热编码
* @sang
 */

object Test13 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test13")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val df = spark.createDataFrame(Seq(
      (0, 0),
      (1, 1),
      (2, 2),
      (3, 1),
      (4, 1),
      (5, 2)
    )).toDF("index", "categoryIndex")

//  创建onehotencoder
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("onehostencoder")

    val model = encoder.transform(df)

    model.show()


  }
}