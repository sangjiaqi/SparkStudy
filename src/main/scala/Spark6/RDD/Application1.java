package Spark6.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application1 {

    public static void main(String[] args) {

//        初始化Spark
        SparkConf conf = new SparkConf()
                .setAppName("Application1")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
        *弹性分布式数据集RDD
         */
//        并行化集合
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

//        外部数据集
        JavaRDD<String> lines = sc.textFile("/home/sangjiaqi/Desktop/Study/Java/Chapter7/src/order/sort7.java");

        /*
        * RDD操作
         */
//        基本操作
        JavaRDD<Integer> lineLengths = lines.map((Function<String, Integer>) String::length);
        int totalLength = lineLengths.reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);
        System.out.println(totalLength);

//        面向对象操作
        JavaRDD<Integer> lineLengths2 = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        int totalLength2 = lineLengths2.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(totalLength2);

//        使用键值对
        JavaPairRDD<String, Integer> pairs = lines.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);
        System.out.println(counts.collect());

//        广播变量
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
        broadcastVar.value();

//        累加器
        LongAccumulator accum = sc.sc().longAccumulator();
        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        System.out.println(accum.value());



    }

}
