package Spark2.Chapter_6Six

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DoubleType

/*
* @ScalaAggregateFunction
* @sang
 */
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.{DataType, StructType}

class ScalaAggregateFunction extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType().add("sales", DoubleType)

  override def bufferSchema: StructType = new StructType().add("sumLargeSales", DoubleType)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0.0)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sum = buffer.getDouble(0)
    if (!input.isNullAt(0)) {
      val sales = input.getDouble(0)
      if (sales > 500.0) {
        buffer.update(0, sum + sales)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}

case class Customer(id: Integer, name: String, sales: Double, discounts: Double, state: String)

object Test4 {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("MyUDAF1").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    import sqlContext.implicits._
//
//    val custs = Seq(Customer(1, "Widget Co", 120200.00, 0.00, "AZ"),
//      Customer(2, "Acme WIdgets", 410600.00, 560.00, "CA"))
//
//    val customerDF = sc.parallelize(custs).toDF()
//
//    customerDF.printSchema()
//
//    customerDF.registerTempTable("customerTable")
//
//    val mysum = new ScalaAggregateFunction()
//
//    sqlContext.udf.register("mysum", mysum)
//
//    val sqlResult = sqlContext.sql("select state, mysum(sales) as bigsales from customerTable group by state")
//
//    sqlResult.printSchema()
//
//    sqlResult.show()
//
//    sc.stop()



  }
}