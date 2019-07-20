package Spark4.Chapter_4Four

import org.apache.spark.graphx.Graph

class ToGexf {

  /**
    * 输出为gexf格式
    * @param g：图
    * @tparam VD
    * @tparam ED
    * @return
    */
  def toGexf[VD,ED](g:Graph[VD,ED]) ={
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n  " +
      "<nodes>\n " +
      g.vertices.map(v => "  <node id=\""+v._1+"\" label=\""+v._2+"\" />\n").collect().mkString+
      "</nodes>\n  "+
      "<edges>\n"+
      g.edges.map(e => "  <edge source=\""+e.srcId+"\" target=\""+e.dstId+"\" weight=\""+e.attr+"\"/>\n").
        collect().mkString+
      "</edges>\n        </graph>\n      </gexf>"

  }

}