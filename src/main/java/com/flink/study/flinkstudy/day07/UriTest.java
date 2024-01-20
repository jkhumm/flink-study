package com.flink.study.flinkstudy.day07;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @date 2024/1/20 16:20
 * @description 使用DataStreamSource，统计出每一个url被访问了多少次
 */
@Slf4j
public class UriTest {


    /**
     *  KeyedProcessFunction ValueState 根据key状态管理来进行+1操作
     *  第一个参数：是用来分组的数据的数据类型，
     *  第二个参数：输入数据的类型
     *  第三个参数：输出数据的类型
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
     *  KeyedProcessFunction MapState 根据key状态管理来进行+1操作
     *  第一个参数：是用来分组的数据的数据类型，
     *  第二个参数：输入数据的类型
     *  第三个参数：输出数据的类型
     */
    public static class MyStatusUpdateFunction2 extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        // 第一个参数是传入的数据，第二个参数是表示数据当前时间和状态的对象（包括数据的事件时间等内容），第三个收集器是返回的数据和类型
        @Override
        public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 初始化键控的状态
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("url visit count", String.class, Integer.class);
            MapState<String, Integer> mapState = getRuntimeContext().getMapState(descriptor);
            int count = mapState.get(value.f0) == null ? 0 : mapState.get(value.f0);
            mapState.put(value.f0, count+1);
            System.out.println("mapState的key为：" + value.f0 + "，value为：" + mapState.get(value.f0));
            //System.out.println(mapState.entries());
        }
    }

    /**
     * 自定义一个映射类型来管理例子和方法
     */
    public static void func() throws Exception {
        String split = "\"url\":";
        String path = "D:\\code\\idea\\flink-study\\files\\day07\\urls.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行设置为1，便于观察，否则会乱序读提升效率
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);
        dataStreamSource.filter(value -> StrUtil.isNotBlank(value) && value.contains(split)).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return Tuple2.of(value.split(split)[1].replace("\"",""), 1);
            }
        }).keyBy(t -> t.f0)
                //.process(new MyStatusUpdateFunction())
                .process(new MyStatusUpdateFunction2())
                .print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        func();
    }


}
