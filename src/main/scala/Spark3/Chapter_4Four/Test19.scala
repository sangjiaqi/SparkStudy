package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.{MaxAbsScaler, StandardScaler}
import org.apache.spark.sql.SparkSession

/*
* @MaxAbsScaler
* @标准化，减均值除以标准差
* @sang
 */

object Test19 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test19")
      .master("local[*]")
      .getOrCreate()

    //  创建数据集
    val data = spark
      .read.
      format("libsvm").
      load("E://BaiduYunDownload//大数据//Data//SparkLearning-master//file//data//mllib//input//mllibFromSpark//sample_libsvm_data.txt")

    //  创建standardScaler对象
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MaxAbsScalerModel
    val scalerModel = scaler.fit(data)

    // rescale each feature to range [-1, 1]
    val scaledData = scalerModel.transform(data)
    scaledData.select("features", "scaledFeatures").show()

  }
}