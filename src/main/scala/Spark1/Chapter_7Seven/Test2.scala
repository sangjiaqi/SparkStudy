package Spark1.Chapter_7Seven

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultJsonFormats

import scala.collection.mutable


//object Test2 {
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    ssc.checkpoint("data/checkpoint")
//
//    val rddQuue = new mutable.SynchronizedQueue[RDD[String]]()
//    val stream = ssc.queueStream(rddQuue)
//
//    val traces = stream.map(_.parseJson.convertTo[ViewTrace])
//    val pw = traces.map(x => (x.productType, 1))
//  }
//}
//
//case class ViewTrace(user: String, action: String, productId: String, productType: String)
//
//object ViewTraceProtocol extends DefaultJsonProtocol {
//  implicit val viewTraceFormat = jsonFormat4(ViewTrace)
//}