package Spark6.DataFrame;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class Application1 {

    public static void main(String[] args) throws AnalysisException {

        /**
         * 入门
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("Application1")
                .master("local[*]")
                .getOrCreate();

//        读取并创建DataFrame
        Dataset<Row> df1 = spark
                .read()
                .json("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.json");

        df1.show();

//        DataFrame操作
        df1.printSchema();
        df1.select("name").show();
        df1.select(df1.col("name"), df1.col("age").plus(1)).show();
        df1.filter(df1.col("age").gt(21)).show();
        df1.groupBy("age").count().show();

//        SQL查询
        df1.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();

        df1.createGlobalTempView("people");
        spark.sql("select * from global_temp.people").show();
        spark.newSession().sql("select * from global_temp.people");

//        创建Datasets
        Person person = new Person();
        person.setName("Sang");
        person.setAge(32);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        Dataset<Person> peopleDS = spark
                .read()
                .json("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.json")
                .as(personEncoder);
        peopleDS.show();

//        DataFrame与RDD互相操作
        JavaRDD<Person> peopleRDD = spark
                .read()
                .textFile("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person persons = new Person();
                    persons.setName(parts[0]);
                    persons.setAge(Integer.parseInt(parts[1].trim()));
                    return persons;
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        teenagersDF.show();

//        以编程方式指定架构
        JavaRDD<String> peopleRDD2 = spark
                .read()
                .textFile("/home/sangjiaqi/SoftWare/big-data/spark-1.6.1/examples/src/main/resources/people.txt")
                .javaRDD();

        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldsName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldsName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD2.map(record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        Dataset<Row> results = spark.sql("select name from people");

        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
        namesDS.show();

//        UDAF

    }

}

class Person implements Serializable {

    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

}