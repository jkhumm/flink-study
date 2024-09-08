package com.flink.study.day12.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        //读文件
        JavaRDD<String> lines = sparkContext.textFile("D:\\code\\idea\\flink-study\\src\\main\\java\\com\\flink\\study\\day12\\count\\word.txt").cache();
        //读取行，并将单词成集合
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        //map
        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
       //reduce
        JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value, Integer toValue) {
                return value + toValue;
            }
        });
        //控制台打印
        wordsCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println( tuple._1()+" "+tuple._2());
            }
        });

    }
}
