package Spark6.DataFrame;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application2 {

    public static void main(String[] args) {

//        创建spark对象
        SparkSession spark = SparkSession
                .builder()
                .appName("Application2")
                .master("local[*]")
                .getOrCreate();

        /**
         * 数据源本地读取与保存
         */
        Dataset<Row> usersDF = spark
                .read()
                .load("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/users.parquet");
        usersDF.select("name", "favorite_color")
                .write()
                .save("namesAndFavColors.parquet");

        Dataset<Row> peopleDF = spark
                .read()
                .format("json")
                .load("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

        Dataset<Row> peopleDFCsv = spark
                .read()
                .format("csv")
                .option("sep", ";")
                .option("interSchema", "true")
                .option("header", "true")
                .load("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.csv");
        usersDF.write().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .save("users_with_options.orc");

//        保存为hive表（bucket\sort\partition）
        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");

        usersDF.write().partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet");

        peopleDF.write().partitionBy("favorite_color").bucketBy(42, "name").saveAsTable("people_partitioned_bucketed");

        /**
         * 数据源parquet文件格式
         */
        Dataset<Row> peopleDF2 = spark.read().json("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.json");

        peopleDF2.write().parquet("people.parquet");

        Dataset<Row> parquetFileDF = spark.read().parquet("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.parquet");

        parquetFileDF.createOrReplaceTempView("parquetFile");

        Dataset<Row> namesDF = spark.sql("select name from parquetFile where age between 13 and 19");

        Dataset<String> namesDS = namesDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());

        /**
         * Json文件格式
         */
        Dataset<Row> people = spark.read().json("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.json");

        people.printSchema();

        people.createOrReplaceTempView("people");

        Dataset<Row> namesDF2 = spark.sql("select name from parquetFile where age between 13 and 19");

        namesDF2.show();

        /**
         * Hive表
         */


        /**
         * JDBC
         */





    }

}
