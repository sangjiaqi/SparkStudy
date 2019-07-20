package Spark3.Chapter_5Five

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}

/*
* @Cross-Validation
* @交叉验证调参
* @sang
 */

object Test1 {
  def main(args: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test1")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    training.show()

    test.show()

//  创建模型
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

//  创建调参列表
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

//  创建交叉验证模型
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

//  训练模型
    val cvModel = cv.fit(training)

//  模型评估
    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach{case Row(id: Long, text: String, prob: Any, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

  }
}