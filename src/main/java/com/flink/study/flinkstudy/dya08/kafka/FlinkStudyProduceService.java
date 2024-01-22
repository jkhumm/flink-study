package com.flink.study.flinkstudy.dya08.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
@Slf4j
public class FlinkStudyProduceService {

    @Resource
    @Qualifier("fixCallTemplate")
    private KafkaTemplate<String, byte[]> fixKafkaTemplate;

    public static final String FLINK_STUDY_HUMM_TOPIC = "FLINK_STUDY_HUMM_TOPIC";


    public void testProduce(String req) {
        try {
            // 我这里指定了分区
            fixKafkaTemplate.send(FLINK_STUDY_HUMM_TOPIC, 0, getRandom(),  req.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e) {
            log.error("fixOrderCallback error", e);
        }
    }

    private String getRandom() {
        return UUID.randomUUID().toString();
    }
}