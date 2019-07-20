package Spark4.Chapter_5Five

import org.apache.spark.{SparkConf, SparkContext}

class Sc {

  def sc(appName: String, master: String): SparkContext = {

    val conf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)

    val sc: SparkContext = new SparkContext(conf)

    return sc

  }

}