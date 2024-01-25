package com.flink.study.day10.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author humingming
 * @date 2024/1/24 21:56
 */
public class KafkaTest {

    public static void func() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.222.132:9092");
        properties.setProperty("group.id", "flink-consume-group");

        //从kafka中读取数据，并且输出到控制台，添加获取数据的主题
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test-topic", new SimpleStringSchema(), properties);


        //将数据放入到数据流中
        DataStream<String> ds = env.addSource(kafkaConsumer);
        DataStream<Tuple2<String,Integer>> rs = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        }).keyBy(t->t.f0).sum(1);
        rs.print();
        env.execute();
    }


    public static void main(String[] args) throws Exception {
        func();
    }

}
