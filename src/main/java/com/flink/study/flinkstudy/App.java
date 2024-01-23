//package com.flink.study.flinkstudy;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.springframework.context.ApplicationContext;
//import org.springframework.stereotype.Service;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//
///**
// * @Author: humingming
// * @Time: 2024/1/22 18:11
// */
//@Service
//public class App {
//
//
//
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 生成一些示例数据
//        DataStream<String> dataStream = env.fromElements("Message 1", "Message 2", "Message 3");
//
//        // 设置 Kafka 生产者的配置
//        String kafkaBootstrapServers = "localhost:9092"; // Kafka 服务器地址
//        String kafkaTopic = "your_kafka_topic"; // Kafka 主题名称
//
//        // 创建 Kafka 生产者
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
//                kafkaTopic,
//                new SimpleStringSchema(),
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 设置 Kafka 语义
//
//        // 设置 Kafka 生产者的配置信息
//        kafkaProducer.setWriteTimestampToKafka(true);
//        kafkaProducer.setLogFailuresOnly(false);
//        kafkaProducer.setFlushOnCheckpoint(true);
//
//        // 将数据发送到 Kafka 主题
//        dataStream.addSink(kafkaProducer);
//
//        // 执行 Flink 作业
//        env.execute("Flink Kafka Producer Example");
//    }
//
//
//}
