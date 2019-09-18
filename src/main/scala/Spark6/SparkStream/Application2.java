package Spark6.SparkStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application2 {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
            .setAppName("Application2")
            .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<Row> sample = sc.parallelize(Arrays.asList("SangJiaQi 21 100.0"))
                .map(line -> {
                    String[] str = line.split(" ");
                    return RowFactory.create(str[0], str[1], str[2].trim());
                });

        List<StructField> fields = new ArrayList<>();
        StructField field1 = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        StructField field3 = DataTypes.createStructField("grade", DataTypes.DoubleType, true);
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(sample, schema);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        lines.foreachRDD(rdd -> {
            JavaRDD<String[]> rdd1 = rdd.map(line -> line.split(" "));
            JavaRDD<Tuple3<String, String, String>> rdd2 = rdd1.map(line -> new Tuple3<>(line[0], line[1], line[2]));
            JavaRDD<Student> rdd3 = rdd2.map(line -> {
               Student student = new Student();
               student.setName(line._1());
               student.setAge(Integer.parseInt(line._2()));
               student.setGrade(Double.parseDouble(line._3()));
               return student;
            });
            Dataset<Row> dataset = sqlContext.createDataFrame(rdd3, Student.class);
            dataFrame.unionAll(dataset).show();
        });

        ssc.start();
        ssc.awaitTermination();

    }

}

class Student {
    private String name;
    private Integer age;
    private Double grade;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public void setGrade(double grade) {
        this.grade = grade;
    }

    public double getGrade() {
        return grade;
    }

}