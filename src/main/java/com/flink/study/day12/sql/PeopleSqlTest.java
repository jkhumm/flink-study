package com.flink.study.day12.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PeopleSqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("D:\\code\\idea\\flink-study\\files\\day12\\people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("select * from  people where age>20");
        teenagersDF.show();
    }


}
