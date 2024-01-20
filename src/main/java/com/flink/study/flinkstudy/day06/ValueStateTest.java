package com.flink.study.flinkstudy.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author humingming
 * @date 2024/1/18 21:04
 * @description 键控状态之valueState
 */
public class ValueStateTest {
    /**
     * 状态：用于存储和访问流处理中的中间和结果值的
     * 键控状态：keyed State
     *          张三  89
     *          李四  90
     *          王二  91
     *          比如之前用到的方法，根据状态分组 KeyBy(t->t.f0).process(),按照名称分组
     *  valueState
     *
     *
     * 算子状态：Operator State
     *  对于所有流中的键都是共享的数据，适用于维护全局信息的情况，例如累加器或者时间戳等概念
     *
     */


    /**
     * flatmap方式获取单词出现次数
     */
    public static void flatMapFunc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Tuple2<String,Integer>> singleStreamOperator = ds.flatMap((String s, Collector<Tuple2<String,Integer>> collector) -> {
            collector.collect(new Tuple2<>(s,1));
        }).returns(Types.TUPLE(Types.STRING,Types.INT));
        // 按照单词作为key进行分组：两种写法 keyBy groupBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = singleStreamOperator.keyBy(t -> t.f0);
        // 分组后对每个单词进行求和  a,0   a,0    b,1   b,1
        keyedStream.sum(1).print();
        env.execute();
    }

    /**
     * map方式获取单词出现次数
     */
    public static void mapFunc() throws Exception {
        // 使用map重新格式化数据进行统计
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);

        // 设置并行度为1，方便看到结果
        env.setParallelism(1);
        // 创建一个包含英文单词的数据流
        DataStream<String> ds = env.fromElements("hello", "world", "hello", "flink", "world", "hello", "hello", "hello", "flink", "java", "world"
        );

        DataStream<Tuple2<String, Integer>> rs = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                return Tuple2.of(s,1);
            }
        }).keyBy(t->t.f0).sum(1);
        rs.print();
        env.execute();
    }


    /**
     *  第一个参数是用来分组的数据的数据类型，
     *  第二个参数输入数据的类型
     *  第三个输出数据的类型
     */
    public static class WordCountFunc extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        @Override
        // 第一个参数是传入的数据，第二个参数是表示数据当前时间和状态的对象（包括数据的事件时间等内容），第三个收集器是返回的数据和类型
        public void processElement(Tuple2<String, Integer> stringIntegerTuple2,
                                   KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context,
                                   Collector<Tuple2<String, Integer>> collector) throws Exception {
            //初始化键控的状态
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("wordCount", Integer.class);
            //获取当前传入的数据的状态
            ValueState<Integer> counts = getRuntimeContext().getState(descriptor);
            //获取当前传入的这个单词的次数，第一次获取到的单词，对应的值是一个空值
            Integer c = counts.value() == null ? 0 : counts.value();
            c++;
            //更新状态，将刚才发生了变化的值，重新传递到单词对应的分区中
            counts.update(c);
            //通过收集器将数据输出出去
            System.out.println(context.timerService().currentProcessingTime());
            collector.collect(Tuple2.of(stringIntegerTuple2.f0,c));
        }
    }

    /**
     * 使用键控状态 keyed State 进行数据统计
     */
    public static void keyStateFunc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
        DataStream<Tuple2<String, Integer>> rs = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                return Tuple2.of(s,1);
            }
        }).keyBy(t->t.f0)
        // 有着这些中间态数据a,0 a,0   b,1   b,1，再交给WordCountFunc进行你要的业务操作
        .process(new WordCountFunc());
        rs.print();
        env.execute("word count job");
    }


    public static void main(String[] args) throws Exception {
        // flatMapFunc();
        //mapFunc();
        keyStateFunc();
    }


}
