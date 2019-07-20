package Spark5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object Test2 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Test2")
      .getOrCreate()

    import spark.implicits._

//  逻辑
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(ProcessingTime(5000))
      .start()

    query.awaitTermination()

  }

}