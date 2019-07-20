package Spark2.Chapter_6Six

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
* @GeometricMean
* @sang
 */

class GeometricMean extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("count", LongType)::StructField("product", DoubleType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1) , 1.toDouble / buffer.getLong(0))
  }

}

object Test5 {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("MyUDAF2").setMaster("local[*]")
//    val sc = new SparkContext(sc)
//    val sqlContext = new SQLContext(sc)
//
//    val geometricMean = new GeometricMean()
//    sqlContext.udf.register("geometricMean", geometricMean)
//
//    sc.stop()

  }
}