package Spark4.Chapter_5Five

import org.apache.spark.graphx.{Edge, Graph, lib}

/*
* @标签传播
* @sang
 */

object Test5 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test5", "local[*]")

//  创建数据集
    val v = sc.makeRDD((1L to 8L).map((_, "")))
    val e = sc.makeRDD(Array(Edge(1L, 2L, ""), Edge(2L, 3L, ""), Edge(3L, 4L, ""), Edge(4L, 1L, ""), Edge(1L, 3L, ""), Edge(2L, 4L, ""), Edge(4L, 5L, ""), Edge(5L, 6L, ""), Edge(6L, 7L, ""), Edge(7L, 8L, ""), Edge(8L, 5L, ""), Edge(5L, 7L, ""), Edge(6L, 8L, "")))

//  标签传播算法
    lib.LabelPropagation.run(Graph(v, e), 5)
      .vertices
      .collect()
      .sortWith(_._1 < _._1)

  }

}