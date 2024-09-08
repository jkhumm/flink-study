package com.flink.study.day12.count

import org.apache.spark.sql.SparkSession

/**
 * @author humingming
 * @date 2024/9/4 23:27
 * @description
 */
object ScalaWord {

  def main(args: Array[String]): Unit = {
    println("========begin")
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("word calc count")
      .getOrCreate()
    println("========begin1")
    // val rdd = session.sparkContext.textFile("hdfs://hadoop-master:9000/flink/word.txt")
    val rdd = session.sparkContext.textFile("D:\\code\\idea\\flink-study\\src\\main\\java\\com\\flink\\study\\day12\\count\\word.txt")
    var counts = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    counts.collect().foreach(println)
    println("全部的单词数：" + counts.count())
    counts.saveAsTextFile("D:\\code\\idea\\flink-study\\src\\main\\java\\com\\flink\\study\\day12\\count\\result")

  }

}
