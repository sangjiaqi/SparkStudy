package Spark5

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SparkSession

object Test3 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

//  创建文本流
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    socketDF.isStreaming

    socketDF.printSchema()

//    val userSchema = new StructType()
//      .add("name", "string")
//      .add("age", "integer")

//    val csvDF = spark
//      .readStream
//      .option("sep", ";")
//      .schema(userSchema)
//      .csv("D://")

  }

}