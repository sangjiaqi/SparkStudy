package Spark4.Chapter_5Five

import org.apache.spark.graphx.{Edge, Graph}

/*
* @连通组建
* @sang
 */

object Test4 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test4", "local[*]")

//  创建数据集
    val g = Graph(sc.makeRDD((1L to 7L).map((_, ""))),
      sc.makeRDD(Array(Edge(2L, 5L, ""), Edge(5L, 3L, ""), Edge(3L, 2L, ""), Edge(4L, 5L, ""), Edge(6L, 7L, "")))
    ).cache()

//  连接器算法
    g.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect

//  增强连接器算法
    g.stronglyConnectedComponents(10).vertices.map(_.swap).groupByKey.map(_._2).collect

  }

}