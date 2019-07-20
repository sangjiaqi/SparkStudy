package Spark1.Chapter_8Eight

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Test1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Test2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sql = new SQLContext(sc)

//  示例
    val data = MLUtils.loadLibSVMFile(sc, "C://Users//sangjiaqi//Desktop//软件//Scala//mllib//sample_libsvm_data.txt")

    val splits = data.randomSplit(Array(0.6, 0.4),  seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    model.clearThreshold()

    val scoreAndLabels = test.map{point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)

    model.save(sc, "C://Users//sangjiaqi//Desktop//软件//Scala//mllib")
    val sameModel = SVMModel.load(sc, "C://Users//sangjiaqi//Desktop//软件//Scala//mllib")
    println(sameModel)





  }
}