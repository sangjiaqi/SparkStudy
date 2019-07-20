package Spark4.Chapter_4Four

import java.io.PrintWriter
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf().setAppName("Test1").setMaster("local[*]")
    val sc = new SparkContext(conf)

//  生成图
//    确定的图
    util.GraphGenerators.gridGraph(sc, 4, 4)

    util.GraphGenerators.starGraph(sc, 8)

//    随机图
    util.GraphGenerators.logNormalGraph(sc, 15)

    util.GraphGenerators.rmatGraph(sc, 32, 60)



  }
}