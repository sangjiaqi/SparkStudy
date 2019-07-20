package Spark3.MyMachineLearn

import org.apache.spark
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * This program clean the data of the shipei
  *
  * @version 1.00
  * @author sangjiaqi
  */

object MyML5 {

  def main(args: Array[String]): Unit = {

//    create spark object
    val spark = SparkSession
      .builder()
      .appName("MyML5")
      .master("local[*]")
      .getOrCreate()

//    read the data
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\大学\\论文\\实培计划\\shipei2.csv")

//    clean the data

//    离散变量数字化
    val indexer1 = new StringIndexer()
      .setInputCol("province")
      .setOutputCol("provinceIndex")

    val indexe1 = indexer1.fit(data).transform(data)

    val indexer2 = new StringIndexer()
      .setInputCol("city")
      .setOutputCol("cityIndex")

    val index2 = indexer2.fit(indexe1).transform(indexe1)

    //    分词
    val tokenizer = new Tokenizer()
      .setInputCol("occupation")
      .setOutputCol("word")

    val tokenized = tokenizer.transform(index2)

    //    停用词
    val remover = new StopWordsRemover()
      .setInputCol("word")
      .setOutputCol("filter")

    val removed = remover.transform(tokenized)

    //    向量化
    val hashingTF = new HashingTF()
      .setInputCol("filter")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val featured = hashingTF.transform(removed)

    //    词重要性
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val data2 = idf.fit(featured).transform(featured)
      .select("claim", "pid", "cid", "rid", "year", "month", "day", "sex", "age", "provinceIndex", "cityIndex", "features")

//    write the data
//    data2.select("claim", "pid", "cid", "rid", "year", "month", "day", "sex", "age", "provinceIndex", "cityIndex").write
//      .format("csv")
//      .option("header", "true")
//      .option("delimiter", ",")
//      .save("D:\\大学\\论文\\实培计划\\shipei3.csv")

    data2.select("features").collect().foreach(println)


  }



}