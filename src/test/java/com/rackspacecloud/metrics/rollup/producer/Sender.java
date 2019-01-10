package com.rackspacecloud.metrics.rollup.producer;

import com.rackspace.maas.model.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, Metric> kafkaTemplate;

    public void send(Metric payload, String topic) {
        LOGGER.info("START: Sending payload [{}]", payload);
        kafkaTemplate.send(topic, payload);
        LOGGER.info("FINISH: Processing");
    }
}