package Spark2.Chapter_3Three

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Spark SQL ON Hive
* @sang
 */

object Test1 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf().setAppName("SparkSQLOnHive").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

//  创建表并加载数据
    hiveContext.sql("use test")
    hiveContext.sql("drop table if exists people")
    hiveContext.sql("create table if not exists people(name int, age int)")
    hiveContext.sql("load data local inpath '/Hadoop/Data/SparkSQL/people.txt' into table people")

    hiveContext.sql("drop table if exists peoplescores")
    hiveContext.sql("create table if not exists peoplescores(name int, score int)")
    hiveContext.sql("load data local inpath '/Hadoop/Data/SparkSQL/peoplescores.txt' into table peoplescores")

//  数据表操作
    val resultDF = hiveContext.sql("select pi.name, pi.age, ps.score from people po join peoplescores ps on pi.name = ps.name where ps.score > 90")
  }
}