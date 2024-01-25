package com.flink.study.day10.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.UUID;

@Service
@Slf4j
public class FlinkStudyProduceService {

    @Resource
    private KafkaTemplate<String, String> flinkStudyTemplate;

    public static final String topic = "test-topic";


    public void testProduce(String req) {
        try {
            // 我这里指定了分区
            flinkStudyTemplate.send(topic, getRandom(),  req);
        }catch (Exception e) {
            System.out.println("-----------error kafka send");
            log.error("fixOrderCallback error", e);
        }
    }

    private String getRandom() {
        return UUID.randomUUID().toString();
    }
}