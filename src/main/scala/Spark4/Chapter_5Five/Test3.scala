package Spark4.Chapter_5Five

import org.apache.spark.graphx.{Edge, Graph, lib}

/*
* @查找最少的跳跃：最短路径
* @sang
 */

object Test3 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test3", "local[*]")

//  读取数据集
    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gtm this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)

//  计算最短路径算法
    lib.ShortestPaths.run(myGraph, Array(3)).vertices.collect()



  }

}