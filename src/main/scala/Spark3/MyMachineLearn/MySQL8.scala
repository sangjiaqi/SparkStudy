package Spark3.MyMachineLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object MySQL8 {

  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("MyML2")
      .master("local[*]")
      .getOrCreate()

    //  读取数据
    val order = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D://Data//自考不过保单.csv")
      .withColumnRenamed("order_id", "id")

    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D://Data//自考不过学习行为.csv")

    val data2 = data.rdd.map(x => {
//      保单号
      val order_id = x.getString(0)
//      上课日期
      val class_date = x.getString(1)
//      当日上课时长
      val study_time = x.getDouble(2)
//      回看时长
      val playback_time = x.getDouble(3)

      (order_id, (class_date, class_date, study_time, playback_time))

    }).reduceByKey((x, y) => {
//      开始上课时间
      val min_date = if (x._1 < y._1) x._1 else y._1
//      结束上课时间
      val max_date = if (x._2 > y._2) x._1 else y._1
//      总上课时间
      val sum_study_time = x._3 + y._3
//      总回放时间
      val sum_playback_time = x._4 + y._4

      (min_date, max_date, sum_study_time, sum_playback_time)

    }).filter(x => {
      //      筛选适合月份的保单
      x._1.contains(order.rdd)
    }).map(x => {
      (x._1, x._2._1, x._2._2, x._2._2, x._2._3, x._2._4)
    })

    data2.collect().foreach(println)

  }

}