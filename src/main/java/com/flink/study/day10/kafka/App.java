package com.flink.study.day10.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @Author: humingming
 * @Time: 2024/1/22 18:11
 */
@Slf4j
@SpringBootApplication
public class App {

    public static void main(String[] args) {
        // 启动 Spring Boot 应用程序
        ConfigurableApplicationContext context = SpringApplication.run(App.class, args);

        // 获取 KafkaProducerService bean
        FlinkStudyProduceService kafkaProducerService = context.getBean(FlinkStudyProduceService.class);
        // 发送消息
        for(int i = 0; i < 3; i++) {
            log.info("开始发送消息，第{}条", i);
            kafkaProducerService.testProduce("hello world" + i);
        }

        //context.close();
    }
}
