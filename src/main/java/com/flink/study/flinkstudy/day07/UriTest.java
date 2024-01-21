package com.flink.study.flinkstudy.day07;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @date 2024/1/20 16:20
 * @description 使用DataStreamSource，统计出每一个url被访问了多少次
 */
@Slf4j
public class UriTest {


    /**
     * 自定义一个映射类型来管理例子和方法
     * 一行一行读取，按照split拆分
     */
    public static void func() throws Exception {
        String split = "\"url\":";
        String path = "D:\\code\\idea\\flink-study\\files\\day07\\urls.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行设置为1，便于观察，否则会乱序读提升效率
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);
        dataStreamSource.filter(value -> StrUtil.isNotBlank(value) && value.contains(split))
                // map一对一处理
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) {
                        System.out.println("要处理的数据" + value);
                        return Tuple2.of(value.split(split)[1].replace("\"", ""), 1);
                    }
                }).keyBy(t -> t.f0)
                //.process(new MyStatusUpdateFunction())
                .process(new MyStatusUpdateFunction2())
                .print();
        env.execute();
    }

    /**
     * 一次性和读取数据，然后按照json格式拆分单个对象，分发给process函数
     */
    public static void func2() throws Exception {
        String path = "D:\\code\\idea\\flink-study\\files\\day07\\urls.txt";
        File file = new File(path);
        String content = FileUtils.readFileToString(file,"UTF-8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行设置为1，便于观察，否则会乱序读提升效率
        env.setParallelism(1);
        DataStream<String> ds = env.fromElements(content);
        //对数据格式进行重新的定义，有flatMap()和map()两种方法：flatMap将一个数据拆分成多个数据，map是一个数据重新格式化为另一个数据
        DataStream<Tuple2<String,Integer>> rs = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //使用json的操作方法，对字符串里面的url关键字进行提取值的操作
                //声明一个ObjectMapper的对象
                ObjectMapper om = new ObjectMapper();
                //以树状的方式一层层的读取json的内容
                JsonNode jn = om.readTree(s);
                for(JsonNode j:jn){
                    String text = j.get("url").asText();
                    System.out.println("读取到的数据：" + text);
                    collector.collect(Tuple2.of(text,1));
                }
            }
        }).keyBy(t->t.f0).process(new MyStatusUpdateFunction2());
        env.execute();
    }

    public static void main(String[] args) throws Exception {
       // func();
        func2();
    }

    /**
     * KeyedProcessFunction ValueState 根据key状态管理来进行+1操作
     * 第一个参数：是用来分组的数据的数据类型，
     * 第二个参数：输入数据的类型
     * 第三个参数：输出数据的类型
     */
    public static class MyStatusUpdateFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        // 第一个参数是传入的数据，第二个参数是表示数据当前时间和状态的对象（包括数据的事件时间等内容），第三个收集器是返回的数据和类型
        @Override
        public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 初始化键控的状态
            ValueState<Integer> counts = getRuntimeContext().getState(new ValueStateDescriptor<>("url visit count", Integer.class));
            // 获取当前传入的这个单词的次数，第一次获取到的单词，对应的值是一个空值
            int c = counts.value() == null ? 0 : counts.value();
            c = c + 1;
            // 更新状态，将刚才发生了变化的值，重新传递到单词对应的分区中
            counts.update(c);
            // 通过收集器将数据输出出去，会在控制台显示输出
            out.collect(Tuple2.of(value.f0, c));
            //System.out.println(ctx.timerService().currentProcessingTime());
        }
    }

    /**
     * KeyedProcessFunction MapState 根据key状态管理来进行+1操作
     * 第一个参数：是用来分组的数据的数据类型，
     * 第二个参数：输入数据的类型
     * 第三个参数：输出数据的类型
     */
    public static class MyStatusUpdateFunction2 extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        // 第一个参数是传入的数据，第二个参数是表示数据当前时间和状态的对象（包括数据的事件时间等内容），第三个收集器是返回的数据和类型
        @Override
        public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 初始化键控的状态
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("url visit count", String.class, Integer.class);
            MapState<String, Integer> mapState = getRuntimeContext().getMapState(descriptor);
            int count = mapState.get(value.f0) == null ? 0 : mapState.get(value.f0);
            mapState.put(value.f0, count + 1);
            System.out.println("mapState的key为：" + value.f0 + "，value为：" + mapState.get(value.f0));
            //System.out.println(mapState.entries());
        }
    }


}
