package Spark3.Chapter_10Ten

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/*
* @sparkStreaming
* @sang
 */

object Test1 {

  case class Record(word: String)

  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val conf = new SparkConf()
      .setAppName("Test1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlsc = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(3))

    import sqlsc.implicits._

//  创建sparkStreaming连接
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    words.foreachRDD{ (rdd: RDD[String], time: Time) =>
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      wordsDataFrame.createOrReplaceTempView("words")

      val wordCountsDataFrame = sqlsc.sql("select word, count(*) as total from words group by word")

      println(s"=========$time=========")

      wordCountsDataFrame.show()

    }

    ssc.start()
    ssc.awaitTermination()

  }

}