package Spark4.Chapter_5Five

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}

/*
* @衡量连通性,三角形数
* @sang
 */

object Test2 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test2", "local[*]")

//  读取数据集
    val g = GraphLoader.edgeListFile(sc, "E://BaiduYunDownload//大数据//Data//GraphX//Slashdot0811.txt")

//  三角形关系算法
    val g2 = Graph(g.vertices, g.edges.map(e => if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)))
      .partitionBy(PartitionStrategy.RandomVertexCut)

    (0 to 6).map(i => g2.subgraph(vpred = (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000)
      .triangleCount.vertices.map(_._2).reduce(_ + _))

  }

}