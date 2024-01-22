package com.flink.study.flinkstudy.dya08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 从kafka sensor_topic消费数据，当 sensor_topic 中有新的消息产生时，会先落表 high_temperature_sensor
 * 若，表中满足查询条件（温度大于 30.0）的数据将被插入到 high_temperature_topic 中
 */
public class FlinkSQLKafkaExample {

    public static void main(String[] args) throws Exception {
        // 设置流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 定义 Kafka 输入表
        String sensorTopic = "sensor_topic";
        tEnv.executeSql(
                "CREATE TABLE sensor_data (" +
                        "  sensorId STRING," +
                        "  temperature DOUBLE," +
                        "  create_time TIMESTAMP" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = '" + sensorTopic + "'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'format' = 'json'" +
                        ")"
        );

        // 定义 Kafka 输出表
        String highTemperatureTopic = "high_temperature_topic";
        tEnv.executeSql(
                "CREATE TABLE high_temperature_sensor (" +
                        "  sensorId STRING," +
                        "  temperature DOUBLE," +
                        "  create_time TIMESTAMP" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = '" + highTemperatureTopic + "'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'format' = 'json'," +
                        "  'sink.partitioner' = 'fixed'" +
                        ")"
        );

        // 执行查询
        tEnv.executeSql(
                "INSERT INTO high_temperature_sensor " +
                        "SELECT sensorId, temperature, create_time " +
                        "FROM sensor_data " +
                        "WHERE temperature > 30.0"
        );

        // 打印结果到控制台
        Table resultTable = tEnv.sqlQuery("SELECT * FROM high_temperature_sensor");
        DataStream<Row> resultStream = tEnv.toChangelogStream(resultTable);

        resultStream.print();

        // 执行 Flink 作业
        env.execute("Flink SQL Kafka Example");
    }
}
