package Spark3.Chapter_4Four

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SparkSession

/*
* @SQLTransformer
* @SQL转化器
* @sang
 */

object Test22 {
  def main(args: Array[String]): Unit = {

    //  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test22")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val df = spark.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    sqlTrans.transform(df).show()

  }
}