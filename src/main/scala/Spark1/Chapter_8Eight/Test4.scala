package Spark1.Chapter_8Eight

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/*
线性回归算法
sang
 */
object Test4 {
  def main(args: Array[String]): Unit = {

    val spc = new SparkConf().setAppName("Test4").setMaster("local[*]")

    val sc = new SparkContext(spc)

    val data = sc.textFile("E://BaiduYunDownload//大数据//SparkLearning-master//file//data//mllib//input//ridge-data//lpsa.data")

    val parseData = data.map{ line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

//    parseData.collect().foreach(println)

    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parseData, numIterations)

    val valuesAndPreds = parseData.map {point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)

  }
}