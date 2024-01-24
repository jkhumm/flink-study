package com.flink.study;

import com.flink.study.day10.kafka.FlinkStudyProduceService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author humingming
 * @date 2024/1/25 0:14
 */
@SpringBootTest
public class Test1 {

    @Autowired
    private FlinkStudyProduceService flinkStudyProduceService;


    @Test
    public void test() {
        System.out.println("开始生产数据");
        flinkStudyProduceService.testProduce("123");
    }

}
