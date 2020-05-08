/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.metrics.rollup;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.rollup.models.MetricRollup;
import com.rackspacecloud.metrics.rollup.producer.MockMetricHelper;
import com.rackspacecloud.metrics.rollup.producer.Sender;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@EnableKafka
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:3333", "port=3333"},
        topics = {
        "${kafka.topics.in}",
        "${kafka.topics.out-level-1}"
})
@ActiveProfiles("test")
public class RollupServiceApplicationTests {

    @Autowired
    KafkaEmbedded embeddedKafka;

    @Autowired
    ApplicationContext context;

    @Value("${kafka.topics.in}")
    private String topicIn;

    @Value("${kafka.topics.out-level-1}")
    private String topicOutLevel1;

    @Autowired
    private Sender sender;

	@Test
	public void contextLoads() {
	}

	@Test
    public void testRollup() throws Exception {

        KafkaStreams kafkaStreams = context.getBean(KafkaStreams.class);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        sendBatchesOfMessages(sender, 2, 5);

        Thread.sleep(10*1000L); // wait for a few sec for consumer to process some records

        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("testGroup", "true", this.embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        ConsumerFactory<String, MetricRollup> cf = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(MetricRollup.class));
        Consumer<String, MetricRollup> consumer = cf.createConsumer();

        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topicOutLevel1);
        ConsumerRecords<String, MetricRollup> replies = KafkaTestUtils.getRecords(consumer);
        Assert.assertTrue("Failed to rollup.", replies.count() > 0);

        kafkaStreams.close();
    }

    private void sendBatchesOfMessages(Sender testBean, int batch, int uniqueTagSets) {
        for(int i = 0; i < 1; i++) {
            /**
             * Purpose of 'batch' is to facilitate aggregation. If 'batch' is of value 5, then there will be
             * 5 records available for rollup into 1.
             */
            for (int j = 0; j < batch; j++) {
                runRollupForUniqueTagSets(testBean, uniqueTagSets);
            }
        }
    }

    /**
     * This should send 5 unique messages to kafka. Uniqueness defined by tag sets. These unique tag sets
     * become rollup keys later in the rollup process.
     * @param testBean
     * @param uniqueTagSets
     */
    private void runRollupForUniqueTagSets(Sender testBean, int uniqueTagSets) {
        for (int i = 0; i < uniqueTagSets; i++) {
            Metric metric =
                    MockMetricHelper.getValidMetric(i, "hybrid:1667601", true, true);
            testBean.send(metric, topicIn);
        }
    }
}
