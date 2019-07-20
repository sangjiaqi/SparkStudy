package Spark5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test1 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test1")

    val ssc = new StreamingContext(conf, Seconds(1))

//  创建DStream连接
    val lines = ssc.socketTextStream("localhost", 9999)

//  业务数据流
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)

    wordCount.print()

//  启动DStream
    ssc.start()
    ssc.awaitTermination()

  }

}