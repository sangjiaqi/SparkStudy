package Spark4.Chapter_7Seven

import Spark4.Chapter_5Five.Sc
import org.apache.spark.graphx._

/*
* @SVDPlusPlus
* @sang
 */

object Test1 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test1", "local[*]")

//  创建数据集
    val edges = sc.makeRDD(Array(
      Edge(1L, 11L, 5.0), Edge(1L, 12L, 4.0), Edge(2L, 12L, 5.0),
      Edge(2L, 13L, 5.0), Edge(3L, 11L, 5.0), Edge(3L, 13L, 2.0),
      Edge(4L, 11L, 4.0), Edge(4L, 12L, 4.0)
    ))

    val conf = new lib.SVDPlusPlus.Conf(2, 10, 0, 5, 0.007, 0.007, 0.005, 0.015)

    val (g, mean) = lib.SVDPlusPlus.run(edges, conf)

    val result = pred(g, mean, 4L, 13L)

    println(result)

  }

  def pred(g: Graph[(Array[Double], Array[Double], Double, Double), Double], mean: Double, u: Long, i: Long) = {

    val user = g.vertices.filter(_._1 == u).collect()(0)._2

    val item = g.vertices.filter(_._1 == i).collect()(0)._2

    mean + user._3 + item._3 + item._1.zip(user._2).map(x => x._1 * x._2).reduce(_ + _)

  }

}