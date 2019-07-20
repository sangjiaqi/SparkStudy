package Spark3.Chapter_10Ten

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {

  def main(args: Array[String]): Unit = {

//  创建spark对象
    val conf = new SparkConf()
      .setAppName("Test2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

//  读取数据集
    val stream = ssc.textFileStream("C://Users//sangjiaqi//Desktop//软件//Scala//mllib//traindir")

//  处理数据
    val NumFeatures = 11
    val zeroVector = DenseVector.zeros[Double](NumFeatures)

//  创建模型
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(20)
      .setRegParam(0.8)
      .setStepSize(0.01)

//  创建含标签的数据流
    val labeledStream = stream.map{ line =>
      val split = line.split(";")
      val y = split(11).toDouble
      val features = split.slice(0, 11).map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

//  启动数据流
    ssc.start()
    ssc.awaitTermination()

  }

}