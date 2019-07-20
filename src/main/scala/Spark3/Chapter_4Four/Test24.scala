package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

/*
* @QuantileDiscretizer
* @分位数离散化
* @sang
 */

object Test24 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test24")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show(false)


  }
}