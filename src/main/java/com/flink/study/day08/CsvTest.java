package com.flink.study.day08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class CsvTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tnv = StreamTableEnvironment.create(env);

        CsvTableSource csvTableSource = CsvTableSource.builder()
                        .path("E:/userinfo.txt")
                        .fieldDelimiter(",")
                        .ignoreFirstLine()
                        .field("userid", DataTypes.INT())
                        .field("username",DataTypes.STRING())
                        .field("age",DataTypes.INT()).build();
        DataStream<Row> s = csvTableSource.getDataStream(env);
        tnv.createTemporaryView("users",s);
        Table result = tnv.sqlQuery("select * from users");
        tnv.toDataStream(result, Row.class).print();
        env.execute();
    }
}
