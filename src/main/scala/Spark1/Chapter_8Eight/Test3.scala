package Spark1.Chapter_8Eight

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/*

 */
object Test3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test3").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val data1 = sc.textFile("C://Users//sangjiaqi//Desktop//软件//Scala///mllib//insurence2.csv")

    data1.map(_.split(",").drop(0)).collect().foreach(println)

    val labelpoint1 = data1.map {line =>
      val tmp = line.split(",")
      LabeledPoint(tmp(0).toInt,Vectors.dense(line.split(",").drop(0).map(_.toDouble)))
    }

    labelpoint1.foreach(println)
  }
}