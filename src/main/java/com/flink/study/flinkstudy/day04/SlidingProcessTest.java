package com.flink.study.flinkstudy.day04;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * @author humingming
 * @date 2024/1/16 21:11
 * @description 滚动窗口 + 滑动窗口
 */
public class SlidingProcessTest {

    // 每秒生成一个参数，整数的范围是1-10，持续生成它们
    public static class RandomIntSource implements SourceFunction<Integer> {

        boolean status = true;

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            // 创建个随机对象
            Random random = new Random();
            while (status) {
                int i = random.nextInt(10) + 1;
                System.out.println("当前生成的数字：" + i);
                // 将数据放入到收集器
                sourceContext.collect(i);
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            status = false;
        }
    }



    public static void func() throws Exception {
        // 初始化flink环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取随机产生的数据，因为是自定义数据，所以用addSource,在里面编写生成数据的方法
        DataStreamSource<Integer> dataSource = environment.addSource(new RandomIntSource());

        // 滚动窗口（统计前面5秒输出的数据之和）
        DataStream<Integer> gundong1 = dataSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0);
        // 滚动窗口（每5个数据统计一次，工作基本不会用）
        DataStream<Integer> gundong2 = dataSource.countWindowAll(5).sum(0);

        // 滑动窗口(每隔5s统计前面10s的数据之和)
        DataStream<Integer> slide1 = dataSource.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum(0);
        DataStream<Integer> slide2 = dataSource.keyBy(v -> v % 2) // keyBy分组 v是dataSource的泛型
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum(0);


        slide2.print();
        environment.execute();
    }

    public static void main(String[] args) throws Exception {
        func();
    }



}
