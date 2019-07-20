package Spark3.Chapter_4Four

import java.util

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/*
* @VectorSlicer
* @特征向量选择
* @sang
 */

object Test25 {
  def main(arga: Array[String]): Unit = {

//  创建spark对象
    val spark = SparkSession
      .builder()
      .appName("Test25")
      .master("local[*]")
      .getOrCreate()

//  创建数据集
    val data = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    dataset.show()

    val slicer = new VectorSlicer()
      .setInputCol("userFeatures")
      .setOutputCol("features")
      .setIndices(Array(1))
      .setNames(Array("f3"))

    val output = slicer.transform(dataset)
    output.show()


  }
}