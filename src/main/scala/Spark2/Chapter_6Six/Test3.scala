package Spark2.Chapter_6Six

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Str2Int
* @sang
 */

object Test3 {
//  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyUDF3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    case class Simple(str: String)
    val data = Seq(Simple("12"), Simple("34"), Simple("56"), Simple("78"))
    sc.parallelize(data).toDF().registerTempTable("Simple")

    sqlContext.udf.register("str2Int", (s: String) => s.toInt)

    sqlContext.sql("select str2Int(str) as str2Int from Simple").show()

    sqlContext.sql("select cast(str as Int) from Simple")

    sc.stop()

//  }
}