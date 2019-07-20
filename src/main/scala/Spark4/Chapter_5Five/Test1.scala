package Spark4.Chapter_5Five

import Spark4.Chapter_4Four.ToGexf
import org.apache.spark.graphx.GraphLoader

/*
* @网页排名
* @sang
 */

object Test1 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val sc = new Sc().sc("Test1", "local[*]")

//  读取并创建图
    val g = GraphLoader.edgeListFile(sc, "E://BaiduYunDownload//大数据//Data//GraphX//Cit-HepTh.txt")

//  个性化PageRank算法
    g.personalizedPageRank(9207016, 0.001)
      .vertices
      .filter(_._1 != 9207016)
      .reduce((a, b) => if (a._2 > b._2) a else b)

  }

}