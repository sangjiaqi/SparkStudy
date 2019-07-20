package Spark4.Chapter_4Four

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf().setAppName("Test2").setMaster("local[*]")

    val sc = new SparkContext(conf)

//  创建图对象
    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gtm this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)

//  序列化、反序列化
    myGraph.vertices.saveAsObjectFile("hdfs://Master:8020/myGraphVertices")
    myGraph.edges.saveAsObjectFile("hdfs://Master:8020/myGraphEdges")

    val myGraph2 = Graph(
      sc.objectFile[Tuple2[VertexId, String]]("myGraphVertices"),
      sc.objectFile[Edge[String]]("myGraphEdges")
    )

//  保存呈json文件序列化
    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsObjectFile("myGraphVertices")

//  高性能json序列化
    import com.fasterxml.jackson.core.`type`.TypeReference
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsObjectFile("myGraphVertices")

    myGraph.vertices.mapPartitions(vertices => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      vertices.map(v => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, v)
        writer.toString
      })
    }).coalesce(1, true).saveAsObjectFile("myGraphVertices")

    myGraph.edges.mapPartitions(edges => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      edges.map(e => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, e)
        writer.toString
      })
    }).coalesce(1, true).saveAsObjectFile("myGraphEdges")

    val myGraph3 = Graph(
      sc.textFile("myGraphVertices").mapPartitions(vertices => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        vertices.map(v => {
          val r = mapper.readValue[Tuple2[Integer, String]](v, new TypeReference[Tuple2[Integer, String]]{})
          (r._1.toLong, r._2)
        })
      }),
      sc.textFile("myGraphEdges").mapPartitions(edges => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        edges.map(e => mapper.readValue[Edge[String]](e, new TypeReference[Edge[String]]{}))
      })
    )



  }

}