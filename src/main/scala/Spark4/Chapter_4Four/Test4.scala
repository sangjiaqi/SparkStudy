package Spark4.Chapter_4Four

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val conf = new SparkConf()
      .setAppName("Test1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //  创建图对象
    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gtm this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)

//  Pregel
    val g = Pregel(
      myGraph.mapVertices((vid, vd) => 0),
      0,
      activeDirection = EdgeDirection.Out
    ) (
      (id: VertexId, vd: Int, a: Int) => math.max(vd, a),
      (et: EdgeTriplet[Int, String]) => Iterator((et.dstId, et.srcAttr + 1)),
      (a: Int, b: Int) => math.max(a, b)
    )

    g.vertices.collect().foreach(println)

  }
}