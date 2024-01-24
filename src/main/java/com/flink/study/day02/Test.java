package com.flink.study.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author humingming
 * @date 2024/1/11 21:28
 * @description
 */
public class Test {


    public static void main(String[] args) throws Exception {
        Test test = new Test();
        test.getDataBySocket();
    }


    /**
     * 从网络中读取数据，进行计数
     */
    public void getDataBySocket() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9000);
        // 在解压后的文件夹所在的窗口中运行cmd命令,nc -lp 9000
        // 根据tuple的第一个元素进行分组，然后按照key进行分组。对第二列进行求和 name:23
        source.flatMap(new myFunc()).keyBy(tuple -> tuple.f0).sum(1).print();

        env.execute();
    }

    /**
     * 二元组 key：单词 value: 默认值1次
     */
    public class myFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            Tuple2<String, Integer> tuple2 = Tuple2.of(s, 1);
            collector.collect(tuple2);
        }
    }


}
