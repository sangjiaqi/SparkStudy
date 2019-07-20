package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

/*
* @StandardScaler
* @标准化，减均值除以标准差
* @sang
 */

object Test17 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test17")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = spark
      .read.
      format("libsvm").
      load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_libsvm_data.txt")

//  创建standardScaler对象
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)

    val scaled = scaler.fit(data).transform(data)
    scaled.show()

  }
}