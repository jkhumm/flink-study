package com.flink.study.day11;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class get_ecs_info {
    //定义表格的结构
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class rate_of_mem {
        Integer mem_total_nums;
        Integer mem_order_nums;
        Integer order_nums;
        Float buy_rate;
        String current_dt;
    };

    public static void main(String[] args) throws Exception {
        //第一个部分，先读取kafka的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置kafka的连接
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.222.132:9092");
        properties.setProperty("group.id","get_shopping_info");
        //读取kafka
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>("order-topic",new SimpleStringSchema(),properties);
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        kafkaStream.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                // 数据：{"order_nums":4,"current_dt":"2024-01-27T09:18:03.000Z","mem_total_nums":2,"buy_rate":0.5000,"mem_order_nums":1}
                //提取kafka读取过来的行数据，每一行都是一个json格式，所以需要通过键值对的方式进行字段的提取
                System.out.println("数据：" + value);
                ObjectMapper objectMapper = new ObjectMapper();
                //json转换成树状格式
                JsonNode jsonNode = objectMapper.readTree(value);
                //分别将五个字段对应的值取出来
                String mem_total_nums = jsonNode.get("mem_total_nums").asText();
                String order_nums = jsonNode.get("order_nums").asText();
                String mem_order_nums = jsonNode.get("mem_order_nums").asText();
                String buy_rate = jsonNode.get("buy_rate").asText();
                String current_dt = jsonNode.get("current_dt").asText();
                //连接mysql数据库
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dws","root","123456");
                String sql = "insert into rate_of_mem values(?,?,?,?,?)";
                PreparedStatement ps = conn.prepareCall(sql);
                ps.setString(1,mem_total_nums);
                ps.setString(2,order_nums);
                ps.setString(3,mem_order_nums);
                ps.setString(4,buy_rate);
                ps.setString(5,current_dt);
                //进行DML语句的提交
                ps.executeUpdate();
                conn.close();
            }
        });
        env.execute();
    }
}
