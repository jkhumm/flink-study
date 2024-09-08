package com.flink.study.day12.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("file:///D:\\code\\idea\\flink-study\\files\\day12\\people.json");
        df.show();
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("select * from  people where age>20");
        sqlDF.show();
    }
}
