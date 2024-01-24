package com.flink.study.day10.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FlinkStudyConsumeService {

    private static final String TOPIC = "test-topic";


    @KafkaListener(topics = TOPIC, groupId = "consume_flink", concurrency = "5")
    public void consume(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();
        System.out.println("key:" + key + "value:" + value);
    }
}