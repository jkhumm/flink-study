package com.flink.study.flinkstudy.day07;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author humingming
 * @date 2024/1/20 14:13
 * @description 算子状态
 */
@Slf4j
public class ListStateTest {

    public static void func() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
        DataStreamSink<String> dataStreamSink = ds.addSink(new MySinkFunction());
        dataStreamSink.setParallelism(1);
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        func();
    }

    public static class MySinkFunction implements SinkFunction<String>, CheckpointedFunction {

        // 通过算子状态中的列表状态进行数据的存储，相当于数据副本备份
        ListState<String> listState;

        // 定义用来存储每一次过来的元素和内容，放入到一个缓存的列表中
        List<String> cache = new ArrayList<>();

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("备份快照，发生故障时触发");
            //将cache结果里面的元素保存到状态中，如果计算有故障，那么可以直接从状态中获取最新的数据，是容错机制的一个部分
            for (String ele : cache) {
                listState.add(ele);
            }
        }


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("进入 initializeState 方法");
            //根据CPU的内核的处理器的数量来进行初始化的，和env.setParallelism(1);如果没有写，那么就默认是整个CPU的逻辑处理器个数
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("wordcount", String.class);
            listState = context.getOperatorStateStore().getListState(descriptor);
            // 判断这个列表中有没有数据，如果有，读取数据，将所有的状态中的数据写入到当前的列表中进行保存
            // context.isRestored()这个方法是在数据发生了故障的时候生效的方法，那么数据就是来自于检查点，
            // 如果发生了故障，那么就会从快照保存的liststate中去获取最新的数据，恢复的数据插入到缓存列表
            if (context.isRestored()) {
                for (String ele : listState.get()) {
                    cache.add(ele);
                }
            }
        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            //将每一次过来的数据，都放入到列表中进行缓存
            cache.add(value);
            System.out.println("invoke：线程id:" + Thread.currentThread().getId() + "，当前写入的数据是：" + value);
            //从传入的数据中中取出内容，放入到bufferedEelements数组中进行数据的集中
            System.out.println("invoke：当前的数据一共有" + cache.size() + "个单词，总览：" + cache);
        }
    }


}
