package com.flink.study.day06;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author humingming
 * @date 2024/1/20 10:33
 * @description 键控状态之MapState
 */
@Slf4j
public class MapStateTest {


    /**
     * 入参类型，出参类型
     */
    public static class MyFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        @Override
        public void processElement(Tuple2<String, Integer> value, ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 生成一个你要定义的mapState的类型结构
            MapStateDescriptor<String, Integer> mapStateDescriptor =  new MapStateDescriptor<>("count", String.class, Integer.class);
            MapState<String, Integer> mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            // 就相当于你定义了一个空的Map<String,Integer>，然后map.get(单词)，然后有数据过来了，比如apple 就map.get("apple"，1),在过来就map.get("apple"，1+1)
            int count = mapState.get(value.f0) == null ? 0 : mapState.get(value.f0);
            mapState.put(value.f0, count+1);
            log.info(value.f0, mapState.get(value.f0));
            System.out.println(mapState.entries());
        }
    }


    /**
     * 自定义一个映射类型来管理例子和方法
     */
    public static void func() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return Tuple2.of(value, 1);
            }
        });
        // 按照单词分组keyBy a,1 a,1  这里之所以要用keyBy是因为MapState 必须要分组
        // Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.
        operator.keyBy(f -> f.f0).process(new MyFunction()).print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        func();
    }


}
