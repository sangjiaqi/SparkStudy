package Spark4.Chapter_6Six

import Spark4.Chapter_5Five.Sc
import org.apache.spark.graphx._

/*
* @基于word2Vec的最小生成树
* @sang
 */

object Test4 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test4", "local[*]")

//  构建距离图
    val dist = sc.textFile("animal_distances.txt")
    val verts = dist.map(_.split(",")(0))
      .distinct()
      .map(x => (x.hashCode.toLong, x))

    val edges = dist.map(x => x.split(","))
        .map(x => Edge(x(0).hashCode.toLong,x(1).hashCode.toLong, x(2).hashCode.toDouble))

    val distg = Graph(verts, edges)

    val mst = minSpanningTree(distg)

  }

  def minSpanningTree[VD: scala.reflect.ClassTag](g: Graph[VD, Double]) = {

    //  初始化
    var g2 = g.mapEdges(e => (e.attr, false))

    for (i <- 1L to g.vertices.count - 1) {
      val unavailableEdges = g2.outerJoinVertices(g2.subgraph(_.attr._2)
        .connectedComponents()
        .vertices
      ) ((vid, vd, cid) => (vd, cid)).subgraph(et => et.srcAttr._2.getOrElse(-1) == et.dstAttr._2.getOrElse(-2))
        .edges
        .map(e => ((e.srcId, e.dstId), e.attr))

      type edgeType = ((VertexId, VertexId), Double)

      //    查找最小权重边
      val smallestEdge = g2.edges
        .map(e => ((e.srcId, e.dstId), e.attr))
        .leftOuterJoin(unavailableEdges)
        .filter(x => !x._2._1._2 && x._2._2.isEmpty)
        .map(x => (x._1, x._2._1._1))
        .min()(new Ordering[edgeType]() {
          override def compare(a: edgeType, b: edgeType) = {
            val r = Ordering[Double].compare(a._2, b._2)
            if (r == 0)
              Ordering[Long].compare(a._1._1, b._1._1)
            else
              r
          }
        })

      g2 = g2.mapTriplets(et => (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1 && et.dstId == smallestEdge._1._2)))

    }

    g2.subgraph(_.attr._2).mapEdges(_.attr._1)

  }

}