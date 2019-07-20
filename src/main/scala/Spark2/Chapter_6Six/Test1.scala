package Spark2.Chapter_6Six

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Hobby_count
* @sang
 */

object Test1 {
//  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf().setAppName("MyUDF1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//  隐式转换
    import sqlContext.implicits._

    case class NameHobbies(name: String, hobbies: String)

    val data = Seq(NameHobbies("sasuke", "jog,code,cook"),NameHobbies("naruto", "travel,dance"))

    sc.parallelize(data).toDF().registerTempTable("NameHobbiesTable")

    sqlContext.udf.register("hobby_count", (s: String) => s.split(",").size)

    sqlContext.sql("select * , hobby_count(hobbies) as hobby_count from NameHobbiesTable").show()

    sc.stop()

//  }
}