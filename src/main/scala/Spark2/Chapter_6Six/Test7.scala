package Spark2.Chapter_6Six

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
* @BelowThreshold
* @sang
 */

class BelowThreshold extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType(Array(StructField("power", IntegerType)))

  override def bufferSchema: StructType = new StructType(Array(StructField("bool", BooleanType)))

  override def dataType: DataType = BooleanType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, false)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer.update(0, buffer.getBoolean(0) || input.getBoolean(0))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getBoolean(0) || buffer2.getBoolean(0))
  }

  override def evaluate(buffer: Row): Any = buffer.getBoolean(0)

}

object Test7 {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("MyUDAF2").setMaster("local[*]")
//    val sc = new SparkContext(sc)
//    val sqlContext = new SQLContext(sc)
//
//    val belowThreshold = new BelowThreshold()
//    sqlContext.udf.register("belowThreshold", belowThreshold)

  }
}