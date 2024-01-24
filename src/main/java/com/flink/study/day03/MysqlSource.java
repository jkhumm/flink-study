package com.flink.study.day03;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

/**
 * Flink 也可以像 sqoop、datax、kettle 等工具一样，实现不同数据库之间的迁移操作，所以使用 Flink 去读取另一个数据库的表格是很常见的操作之一。
 * RichparallelSourceFunction : 多功能并行数据源(并行度能够>=1)，这是用的最多的方法
 */
public class MysqlSource {

    @Data
    @AllArgsConstructor
    public static class Teacher{
        private Integer id;
        private String name;
        private String age;
    }

    // 定义一个mysql数据库表格的读取过程，以多功能并行数据源的方式进行读取
    public static class mysqlDB extends RichParallelSourceFunction<Teacher> {
        private boolean flag = true;            // 定义数据库开始和结束的标记
        private Connection conn = null;         // 定义一个数据库的连接
        private PreparedStatement ps = null;    // 定义一个sql语句的执行过程
        private ResultSet rs = null;            // 定义一个select查询的结果集

        @Override
        public void run(SourceContext<Teacher> sourceContext) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://ip:port/simone","admin","123456");
            // 预加载这个sql语句
            ps = conn.prepareStatement("select * from teacher limit 10");
            // 判断flag的值，只要没有退出程序，就循环一直读取这个表格的数据
            while (flag) {
                rs = ps.executeQuery();
                // 循环读取每一行的结果集数据
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    String age = rs.getString("age");
                    // 将数据放入到收集器中
                    sourceContext.collect(new Teacher(id, name, age));
                }
                // 设置每隔2秒读取一次
                Thread.sleep(2000);
            	// 我准备每次实时抽取数据的时候打印下当前时间，所以导入了时间的部分
                System.out.println(LocalDateTime.now());
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 流式API的传统执行模式我们称之为STREAMING 执行模式(默认), 这种模式一般用于无界流, 需要持续的在线处理 https://www.jianshu.com/p/58e627236b29
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStream<Teacher> sal = env.addSource(new mysqlDB()).setParallelism(1);
        sal.print();
        env.execute();
    }
}
