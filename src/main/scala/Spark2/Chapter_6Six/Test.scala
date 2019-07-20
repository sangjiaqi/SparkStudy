package Spark2.Chapter_6Six

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

/**
  * The program get the mean of the number
 *
  * @version 1.1
  * @author sangjiaqi
  */

class CustomMean extends UserDefinedAggregateFunction {

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
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getLong(1).toDouble
  }

}

object Test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    hiveContext.udf.register("CustomMean", new CustomMean)

  }

}