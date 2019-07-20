package Spark4.Chapter_4Four

import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
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

    //  读取顶点和边
    myGraph.vertices.collect()

    myGraph.edges.collect()

    myGraph.triplets.collect()

    //  mapping操作
    myGraph
      .mapTriplets(t => (t.attr, t.attr == "is-friends-with" && t.srcAttr.toLowerCase().contains("a")))
      .triplets
      .collect()

    //  Map/Reduce
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).collect()

    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).join(myGraph.vertices).collect()

    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).join(myGraph.vertices).map(_._2.swap).collect()

    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).rightOuterJoin(myGraph.vertices).map(_._2.swap).collect()

    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).rightOuterJoin(myGraph.vertices).map(t => (t._2._2, t._2._1.getOrElse(0))).collect()

    //  迭代的Map/Reduce
    def sendMsg(ec: EdgeContext[Int, String, Int]): Unit = {
      ec.sendToDst(ec.srcAttr + 1)
    }

    def mergeMsg(a: Int, b: Int): Int = {
      math.max(a, b)
    }

    def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {

//    生成新的顶点集
      val verts = g.aggregateMessages[Int](sendMsg, mergeMsg)

//    生成更新信息后的图
      val g2 = Graph(verts, g.edges)

//    查看新图与旧图是否有不同
      val check = g2.vertices.join(g.vertices)
        .map(x => x._2._1 - x._2._2)
        .reduce(_ + _)

      if (check > 0)
        propagateEdgeCount(g2)
      else
        g
    }

    val initialGraph = myGraph.mapVertices((_, _) => 0)
    propagateEdgeCount(initialGraph).vertices.collect().foreach(println)

  }
}