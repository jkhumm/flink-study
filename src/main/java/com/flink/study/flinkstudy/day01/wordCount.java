package com.flink.study.flinkstudy.day01;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.operators.UnsortedGrouping;

public class wordCount {
    public static void main(String[] args) throws Exception{
        // 创建一个静态的执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        DataSource<String> lineData = env.readTextFile("files/words.txt");
        // 将单词打散，准备将单词的数据映射成 (word,1) 的格式
        // 通过Collector收集器收集所有的行数据，并且定义每一行都要将单词映射成二元组类型，前面是单词，后面是数字1
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndNum = lineData.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            // 每一行都用空格拆分单词
            String[] words = line.split(" ");
            // 循环读取拆分之后的数据，将每个单词都转换成二元组
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 按照每一个单词进行分组
        UnsortedGrouping<Tuple2<String,Integer>> s = wordAndNum.groupBy(0);
        // 在每个单词的组内进行聚合统计
        AggregateOperator<Tuple2<String,Integer>> sum = s.sum(1);
        // 打印输出
        sum.print();
    }
}