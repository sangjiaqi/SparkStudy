package Spark4.Chapter_5Five

import org.apache.spark.graphx.Graph

object NetWork {

  def main(args: Array[String]): Unit = {

    val sc = new Sc().sc("NetWork", "local[*]")

    val egonets = sc.wholeTextFiles("")
    val egonet_numbers = egonets.map(x => extract(x._1)).collect()
    val egonet_edges = egonets.map(x => make_edges(x._2)).collect()
    val egonet_circles = egonet_edges.toList.map(x => get_circles(x))

    println("UserId, Prediction")

    val result = egonet_numbers.zip(egonet_circles).map(x => x._1 + "," + x._2)
    println(result.mkString("\n"))

  }

  def extract(s: String) = {
    val Pattern = """^.*?(\d+).egonet""".r
    val Pattern(num) = s
    num
  }

  def get_edges_from_line(line: String): Array[(Long, Long)] = {
    val ary = line.split(":")
    val srcId = ary(0).toInt
    val dstIds = ary(1).split(" ")
    val edges = for {
      dstId <- dstIds
      if (dstId != "")
    } yield {
      (srcId.toLong, dstId.toLong)
    }
    if (edges.size > 0) edges else Array((srcId, srcId))
  }

  def make_edges(contents: String) = {
    val lines = contents.split("\n")
    val unflat = for {
      line <- lines
    } yield {
      get_edges_from_line(line)
    }
    val flat = unflat.flatten
    flat
  }

  def get_circles(flat: Array[(Long, Long)]) = {
    val sc = new Sc().sc("NetWork", "local[*]")
    val edges = sc.makeRDD(flat)
    val g = Graph.fromEdgeTuples(edges, 1)
    val cc = g.connectedComponents()
    cc.vertices
      .map(x => (x._2, Array(x._1)))
      .reduceByKey((a, b) => a ++ b)
      .values
      .map(_.mkString(" "))
      .collect()
      .mkString(";")
  }


}