package com.flink.study.flinkstudy.dya08;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

/**
 * @date 2024/1/21 14:16
 * @description flink-sql 操作临时表创建
 */
public class Test {

    @AllArgsConstructor
    @Data
    public static class UserInfo {
        private final String user;
        private final String url;
        private final String createTime;
    }

    /**
     * 参考：https://cloud.tencent.com/developer/article/1983258?areaSource=102001.19&traceId=_d2WPm-qtTJxdacf-2jXI
     * https://developer.aliyun.com/article/980856
     */
    public static void func() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表执⾏环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义数据源，并且自己去设置表格的列名
        Table sourceTable = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("user", DataTypes.STRING()),
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("createTime", DataTypes.STRING())
                ),
                row("Mary", "./home","2022-02-02 12:00:00"),
                row("Bob", "./cart","2022-02-02 12:00:00"),
                row("Mary", "./prod?id=1","2022-02-02 12:00:05"),
                row("Liz", "./home","2022-02-02 12:01:00"),
                row("Bob", "./prod?id=3","2022-02-02 12:01:30"),
                row("Mary", "./prod?id=7","2022-02-02 12:01:45")
        );
        sourceTable.printSchema();

        // 创建虚拟表 通过SQL查询
        tableEnv.createTemporaryView("resultTableView", sourceTable);

        Table tempTable = tableEnv.sqlQuery("select * from resultTableView where user = 'Mary'");

        DataStream<UserInfo> stream = tableEnv.toDataStream(tempTable, UserInfo.class);
        stream.print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        func();
    }

}
