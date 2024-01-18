package com.flink.study.flinkstudy.day02;

import lombok.Data;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;


/**
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)，这是用的最多的方法
 */

@Data
public class RichParallelSourceTest extends RichParallelSourceFunction<User> {
        boolean flag = true;
        @Override
        public void run(SourceContext<User> sourceContext) throws InterruptedException {
            Random random = new Random();
            while (flag) {
                // 随机名字，长度为5，小写字母组成
                String name = "";
                for (int i = 0; i < 5; i++) {
                    name = name + (char)(random.nextInt(26) + 'a');
                }

                // 年龄18-70
                int age = random.nextInt(52) + 18;

                String sex = "";
                if (random.nextInt(2) == 0) {
                    sex = "男";
                }else {
                    sex = "女";
                }
                User user = new User();
                user.setName(name);
                user.setAge(age);
                user.setSex(sex);
                sourceContext.collect(user);
                Thread.sleep(5000);
            }
        }

    @Override
    public void cancel() {
        System.out.println("-----------------stop---------------");
        flag=false;
    }


}