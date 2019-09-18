package Spark6.Start;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Application1 {

    public static void main(String[] args) {

        String logFile = "/home/sangjiaqi/Desktop/Study/Java/Chapter7/src/order/sort7.java";

        SparkSession spark = SparkSession
                .builder()
                .appName("Application1")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> logData = spark
                .read()
                .textFile(logFile)
                .cache();

        long numAs = logData
                .filter((FilterFunction<String>) s -> s.contains("a"))
                .count();

        long numBs = logData
                .filter((FilterFunction<String>) s -> s.contains("b"))
                .count();

        System.out.println("Lines with a: " + numAs + ". lines with b: " + numBs);

        spark.stop();

    }

}
