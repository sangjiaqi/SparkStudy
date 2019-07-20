package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler}
import org.apache.spark.sql.SparkSession

/*
* @MinMaxScaler
* @标准化
* @sang
 */

object Test18 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test18")
      .master("local[*]")
      .getOrCreate()

    //  创建数据集
    val data = spark
      .read.
      format("libsvm").
      load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_libsvm_data.txt")

    //  创建standardScaler对象
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(data)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(data)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("features", "scaledFeatures").show()

  }
}