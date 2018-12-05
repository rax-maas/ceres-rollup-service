package com.rackspacecloud.metrics.rollup.config;

import com.rackspacecloud.metrics.rollup.domain.Metric;
import com.rackspacecloud.metrics.rollup.domain.RolledUp;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJODeserializer;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJOSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class creates and configures topology for the Kafka Streams.
 */
@Configuration
@EnableConfigurationProperties(KafkaConfigurationProperties.class)
public class TopologyConfiguration {
    final KafkaConfigurationProperties configProps;

    @Autowired
    public TopologyConfiguration(KafkaConfigurationProperties kafkaConfigurationProperties){
        this.configProps = kafkaConfigurationProperties;
    }

    @Bean
    Topology topology() {
        final Consumed<String, Metric> consumedAsMetric = getStringMetricConsumed();
        final Produced<String, RolledUp> producedAsMetricRolledUp = getStringRolledUpProduced();

        final StreamsBuilder builder = new StreamsBuilder();

        // Get metric record from kafka topic
        final KStream<String, Metric> source = builder.stream(configProps.getTopics().getIn(), consumedAsMetric);

        // Create stream with Rollup-Key as key and metric record as the value
        KStream<String, Metric> rollupPrepped = source.map(
                (key, metric) -> KeyValue.pair(metric.getRollupKey(), metric)
        );

        // Group records based on rollup-key
        KGroupedStream<String, Metric> groupedStream = rollupPrepped.groupByKey(
                Serialized.with(Serdes.String(), getMetricSerde())
        );

        // Create time-windowed aggregated stream
        long lateArrivalInMinutes = configProps.getStreams().getAggregation().getLateArrivalInMinutes();

        long windowSizeLevel1 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel1();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel1(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel1);

        long windowSizeLevel2 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel2();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel2(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel2);

        long windowSizeLevel3 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel3();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel3(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel3);

        long windowSizeLevel4 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel4();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel4(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel4);

        long windowSizeLevel5 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel5();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel5(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel5);

        Topology topology = builder.build();

//        System.out.println(topology.describe());

        return topology;
    }

    private void rollupForGivenWindowSize(String toTopic,
                                          Produced<String, RolledUp> producedAsMetricRolledUp,
                                          KGroupedStream<String, Metric> groupedStream,
                                          long lateArrivalInMinutes, long windowSize) {

        long retentionPeriod = windowSize + lateArrivalInMinutes;
        KTable<Windowed<String>, Metric> timeWindowedAggregatedStream =
                getWindowedMetricKTable(windowSize, retentionPeriod, groupedStream);

        // Create rolled-up stream
        KStream<String, RolledUp> rolledUpData = timeWindowedAggregatedStream.toStream()
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(),
                        new RolledUp(windowedKey.key(), windowedKey.window().start(),
                                windowedKey.window().end(), value)));

//        rolledUpData.foreach((key, value) -> {
//            ObjectMapper mapper = new ObjectMapper();
//            try {
//                String valueAsString = mapper.writeValueAsString(value);
//                System.out.println(valueAsString);
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        });

        rolledUpData.to(toTopic, producedAsMetricRolledUp);
    }

    private KTable<Windowed<String>, Metric> getWindowedMetricKTable(
            long windowSize, long retentionPeriod, KGroupedStream<String, Metric> groupedStream) {

        TimeWindows timeWindows = TimeWindows
                .of(TimeUnit.MINUTES.toMillis(windowSize))
                .until(TimeUnit.MINUTES.toMillis(retentionPeriod));

        // Aggregating with time-based windowing (tumbling windows)
        return groupedStream.windowedBy(timeWindows).reduce((aggValue, newValue) -> aggValue.reduce(newValue));
    }

    private Consumed<String, Metric> getStringMetricConsumed() {
        Serde<Metric> metricSerde = getMetricSerde();
        return Consumed.with(Serdes.String(), metricSerde);
    }

    private Serde<Metric> getMetricSerde() {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Metric> metricSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Metric.class);
        metricSerializer.configure(serdeProps, false);

        final Deserializer<Metric> metricDeserializer = new JsonPOJODeserializer<>();
        metricDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(metricSerializer, metricDeserializer);
    }

    private Produced<String, RolledUp> getStringRolledUpProduced() {
        Serde<RolledUp> metricRolledUpSerde = getMetricRolledUpSerde();
        return Produced.with(Serdes.String(), metricRolledUpSerde);
    }

    private Serde<RolledUp> getMetricRolledUpSerde() {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<RolledUp> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", RolledUp.class);
        serializer.configure(serdeProps, false);

        final Deserializer<RolledUp> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
