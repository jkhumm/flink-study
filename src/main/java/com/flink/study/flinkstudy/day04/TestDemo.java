package com.flink.study.flinkstudy.day04;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Random;

/**
 * @date 2024/1/16 21:11
 * @description 在1秒内（随机的小于等于1秒）读取一行t0116.txt的数据，统计5秒内行数据全是小写英文的单词数量。
 */
public class TestDemo {

    // 小于等于1秒生成读取文本数据
    public static class RandomIntSource implements SourceFunction<String> {
        boolean status = true;
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            // 创建个随机对象
            Random random = new Random();
            String path = "D:\\code\\idea\\flink-study\\files\\day04\\day04.txt";
            RandomAccessFile file = new RandomAccessFile(path, "r");
            while (status) {
                String line = FileUtil.readLine(file, Charset.defaultCharset());
                System.out.println("读取到的数据：" + line);
                if (StrUtil.isNotBlank(line)) {
                    sourceContext.collect(line);
                    Thread.sleep(random.nextInt(1000));
                }else {
                    status = false;
                    break;
                }
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
        DataStreamSource<String> dataSource = environment.addSource(new RandomIntSource());
        // 滚动窗口（统计前面5秒输出的单词全为小写单词的个数）
        SingleOutputStreamOperator<Integer> streamOperator = dataSource.filter(s -> s.matches("[a-z]+"))
                .flatMap((String line, Collector<Integer> out) -> {
                    // 如果是小写英文就输出1
                    out.collect(1);
                }).returns(Integer.class);
        streamOperator.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0).print();
        environment.execute();
    }
    public static void main(String[] args) throws Exception {
         func();
    }



}
