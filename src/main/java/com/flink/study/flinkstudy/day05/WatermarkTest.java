package com.flink.study.flinkstudy.day05;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

/**
 * @author humingming
 * @date 2024/1/17 21:20
 * @description 滚动窗口求和 + 水位线
 * waterMark = 当前窗口最大时间 - 最大允许的延迟时间
 */
public class WatermarkTest {


    @Data
    @AllArgsConstructor
    static class TimeObj {
        private Integer num;
        private LocalDateTime birthTime;
    }

    // 生成一个随机持续数据流，范围[1,10] 每1秒生成1个，为了模拟延迟到达flink服务器，故意延迟1-4s，而flink
    public static class MyRandomIntSource implements SourceFunction<TimeObj> {

        boolean running = true;

        @Override
        public void run(SourceContext<TimeObj> sourceContext) throws Exception {
            Random r = new Random();
            while (running) {
                LocalDateTime birthTime = LocalDateTime.now();
                int num = r.nextInt(10) + 1;
                TimeObj timeObj = new TimeObj(num, birthTime);
                sourceContext.collectWithTimestamp(timeObj, Timestamp.valueOf(birthTime).getTime());
                System.out.println("生成的数据：" + timeObj);
                // 延迟1-4s到达flink
                int lazyTime = r.nextInt(3001) + 1000;
                Thread.sleep(lazyTime);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static void main(String[] args) throws Exception {
        // 1.创建一个流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<TimeObj> source = env.addSource(new MyRandomIntSource());

        // 添加watermark（时间水位线） ，允许窗口稍后关闭  过期方法建议下面这个
//        DataStream<TimeObj> withWaterMark = source.assignTimestampsAndWatermarks(
//                // 创建一个设置延迟时间的对象，设置延迟时间为3秒，可能会丢失最后1s数据，设置时短但同时服务器响应会更好
//                new BoundedOutOfOrdernessTimestampExtractor<TimeObj>(Time.seconds(3)) {
//                    @Override
//                    public long extractTimestamp(TimeObj obj) {
//                        return obj.getNum();
//                    }
//                });
//        //在水位线的对象上，去引用窗口计算的概念
//        DataStream<Integer> sumStream = withWaterMark.flatMap((TimeObj ob, Collector<Integer> out) -> {
//            out.collect(ob.getNum());
//        }).returns(Integer.class).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0);

        // 创建一个水位线的对象
        WatermarkStrategy<TimeObj> strategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3));
        source.assignTimestampsAndWatermarks(strategy).flatMap((TimeObj ob, Collector<Integer> out) -> {
            out.collect(ob.getNum());
        }).returns(Integer.class).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0).print();
        env.execute();
    }


}
