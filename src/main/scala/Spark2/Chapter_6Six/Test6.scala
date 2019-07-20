package Spark2.Chapter_6Six

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
* @CustomMean
* @sang
 */

class CustomMean2 extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Array(StructField("item", DoubleType)))

  override def bufferSchema: StructType = StructType(Array(StructField("sum", DoubleType), StructField("cnt", LongType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.toDouble
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getLong(1).toDouble
  }
}

object Test6 {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("MyUDAF2").setMaster("local[*]")
//    val sc = new SparkContext(sc)
//    val sqlContext = new SQLContext(sc)
//
//    val customMean = new CustomMean()
//    sqlContext.udf.register("customMeann", customMean)

  }
}