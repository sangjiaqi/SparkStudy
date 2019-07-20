package Spark3.Chapter_9Nine

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

/*
* @FP-Growth
* @频繁挖掘算法
* @sang
 */

object Test2 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test2")
      .master("local[*]")
      .getOrCreate()

//  读取数据集
    import spark.implicits._

    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")

//  创建模型
    val fpgrowth = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.5)
      .setMinConfidence(0.6)

//  训练模型
    val model = fpgrowth.fit(dataset)

//  评估模型
    model.freqItemsets.show()

    model.associationRules.show()

//  测试模型
    model.transform(dataset).show()



  }
}