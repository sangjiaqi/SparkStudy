package Spark2.Chapter_6Six

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Combine
* @sang
 */

object Test2 {
//  def main(args: Array[String]): Unit = {
//  创建spark对象
    val conf = new SparkConf().setAppName("MyUDF2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//  隐式转换
    import sqlContext.implicits._

//  创建数据
    case class NullTable(str1: String, str2: String)
    val data = Seq(NullTable(null, "123"), NullTable("123", "456"))
    sc.parallelize(data).toDF().registerTempTable("NullTable")

//  创建自定义函数
    sqlContext.udf.register("combine", (s1: String, s2: String) => {if (s1 == null) s2 else s1 + s2})

//  执行自定义函数
    sqlContext.sql("select combine(str1, str2) as AB from NummTable").show()

    sc.stop()

//  }
}