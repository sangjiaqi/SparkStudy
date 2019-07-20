package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

/*
* @R formula
* @R formula选择特征与标签
* @sang
 */

object Test26 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test26")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

//  创建R formula
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(dataset).transform(dataset)
    output.show()

  }
}