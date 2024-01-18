package com.flink.study.flinkstudy.day02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author humingming
 * @date 2024/1/11 22:14
 * @description 实现每隔5秒钟，随机的刷新4条用户数据，用户数据中包括随机的名字、随机的年龄和随机的性别
 */
public class Test2 {


    public static void main(String[] args) throws Exception {
        RichParallelSourceTest richParallelSourceTest = new RichParallelSourceTest();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<User> ds = env.addSource(richParallelSourceTest).setParallelism(4);  //setParallelism同时启动多个线程
        ds.print();
        env.execute();
    }

}
