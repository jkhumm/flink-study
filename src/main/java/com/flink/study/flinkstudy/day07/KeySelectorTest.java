package com.flink.study.flinkstudy.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @date 2024/1/21 10:37
 * @description
 */
public class KeySelectorTest {

    /**
     * KeySelector泛型
     * 第一个参数： 你要传入数据的数据类型
     * 第二个参数： 你要分组字段的数据类型
     */
    public static class MySelector implements KeySelector<Tuple2<String, Integer>, String> {
        // 告诉那个是分组字段
        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    /**
     *  第一个参数是用来分组的数据的数据类型，
     *  第二个参数输入数据的类型
     *  第三个输出数据的类型
     */
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        @Override
        public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 生成一个你要定义的mapState的类型结构
            MapStateDescriptor<String, Integer> mapStateDescriptor =  new MapStateDescriptor<>("count", String.class, Integer.class);
            MapState<String, Integer> mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            // 就相当于你定义了一个空的Map<String,Integer>，然后map.get(单词)，然后有数据过来了，比如apple 就map.get("apple"，1),在过来就map.get("apple"，1+1)
            int count = mapState.get(value.f0) == null ? 0 : mapState.get(value.f0);
            mapState.put(value.f0, count+1);
            // System.out.println("key:" + value.f0 + "value:"  + mapState.get(value.f0));
            System.out.println(mapState.entries());
        }
    }


    public static void func() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
        ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(new MySelector()).process(new MyKeyedProcessFunc());
        env.execute();


    }

    public static void main(String[] args) throws Exception {
        func();
    }


}
