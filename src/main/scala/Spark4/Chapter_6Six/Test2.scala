package Spark4.Chapter_6Six

import Spark4.Chapter_5Five.Sc
import org.apache.spark.graphx._

/*
* @推销员问题：贪心算法
* @sang
 */

object Test2 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test2", "local[*]")

//  创建数据集
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0), Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0), Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0), Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)


  }

  def greedy[VD](g: Graph[VD, Double], origin: VertexId) = {

//  初始化图
    var g2: Graph[Boolean, (Double, Boolean)] = g.mapVertices((vid, vd) => vid == origin)
      .mapTriplets(et => (et.attr, false))

//  初始化顶点和边
    var nextVertexId = origin
    var edgesAreAvailable = true
    type tripletType = EdgeTriplet[Boolean, (Double, Boolean)]

    do {

//    更新可用边
      val availableEdges = g2.triplets
        .filter{ et => !et.attr._2 && (et.srcId == nextVertexId && !et.dstAttr || et.dstId == nextVertexId && !et.srcAttr)}

      edgesAreAvailable = availableEdges.count > 0

//    找到最小边并更新顶点
      if (edgesAreAvailable) {
        val smallestEdge = availableEdges.min() (new Ordering[tripletType]() {
          override def compare(a: tripletType, b: tripletType) = {
            Ordering[Double].compare(a.attr._1, b.attr._1)
          }
        })

//      更新顶点
        nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId)
          .filter(_ != nextVertexId)
          .head

//      更新图
        g2 = g2.mapVertices((vid, vd) => vd || vid == nextVertexId)
          .mapTriplets{ et => (et.attr._1, et.attr._2 || (et.srcId == smallestEdge.srcId && et.dstId == smallestEdge.dstId))}

      }

    } while (edgesAreAvailable)



  }

}