package Spark3.Chapter_9Nine

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.sql.SparkSession

/*
* @PrefixSpan
* @频繁挖掘算法
* @sang
 */

object Test3 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

    //  读取数据集
    import spark.implicits._

    val smallTestData = Seq(
      Seq(Seq(1, 2), Seq(3)),
      Seq(Seq(1), Seq(3, 2), Seq(1, 2)),
      Seq(Seq(1, 2), Seq(5)),
      Seq(Seq(6)))

    val df = smallTestData.toDF("sequence")

    //  创建模型
    val ps = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)

    //  训练模型


  }
}