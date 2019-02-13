package com.rackspacecloud.metrics.rollup.config;

import com.rackspacecloud.metrics.rollup.domain.Metric;
import com.rackspacecloud.metrics.rollup.domain.RolledUp;
import com.rackspacecloud.metrics.rollup.serdes.PojoSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
        PojoSerde<Metric> serdeMetric = new PojoSerde<>(Metric.class);
        PojoSerde<RolledUp> serdeRolledUp = new PojoSerde<>(RolledUp.class);

        final Consumed<String, Metric> consumedAsMetric = Consumed.with(Serdes.String(), serdeMetric);
        final Produced<String, RolledUp> producedAsMetricRolledUp = Produced.with(Serdes.String(), serdeRolledUp);

        final StreamsBuilder builder = new StreamsBuilder();

        // Get metric record from kafka topic
        final KStream<String, Metric> source = builder.stream(configProps.getTopics().getIn(), consumedAsMetric);

        // Create stream with Rollup-Key as key and metric record as the value
        KStream<String, Metric> rollupPrepped = source.map(
                (key, metric) -> KeyValue.pair(metric.getRollupKey(), metric)
        );

        // Group records based on rollup-key
        KGroupedStream<String, Metric> groupedStream = rollupPrepped.groupByKey(
                Serialized.with(Serdes.String(), serdeMetric)
        );

        // Create time-windowed aggregated stream
        long lateArrivalInMinutes = configProps.getStreams().getAggregation().getLateArrivalInMinutes();

        level1Rollup(producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes);
        level2Rollup(producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes);
        level3Rollup(producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes);
        level4Rollup(producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes);
        level5Rollup(producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes);

        Topology topology = builder.build();

//        System.out.println(topology.describe());

        return topology;
    }

    /**
     * Level-1 rollup is for 5 minutes rollup of raw data.
     * @param producedAsMetricRolledUp
     * @param groupedStream
     * @param lateArrivalInMinutes
     */
    private void level1Rollup(Produced<String, RolledUp> producedAsMetricRolledUp,
                              KGroupedStream<String, Metric> groupedStream, long lateArrivalInMinutes)
    {
        long windowSizeLevel1 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel1();
        rollupForGivenWindowSize(configProps.getTopics().getOutLevel1(),
                producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel1);
    }

    /**
     * Level-2 rollup is for 20 minutes rollup of raw data.
     * @param producedAsMetricRolledUp
     * @param groupedStream
     * @param lateArrivalInMinutes
     */
    private void level2Rollup(Produced<String, RolledUp> producedAsMetricRolledUp,
                              KGroupedStream<String, Metric> groupedStream, long lateArrivalInMinutes)
    {
        long windowSizeLevel2 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel2();
        if(windowSizeLevel2 > 0) {
            rollupForGivenWindowSize(configProps.getTopics().getOutLevel2(),
                    producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel2);
        }
    }

    /**
     * Level-3 rollup is for 1 hour rollup of raw data.
     * @param producedAsMetricRolledUp
     * @param groupedStream
     * @param lateArrivalInMinutes
     */
    private void level3Rollup(Produced<String, RolledUp> producedAsMetricRolledUp,
                              KGroupedStream<String, Metric> groupedStream, long lateArrivalInMinutes)
    {
        long windowSizeLevel3 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel3();
        if(windowSizeLevel3 > 0) {
            rollupForGivenWindowSize(configProps.getTopics().getOutLevel3(),
                    producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel3);
        }
    }

    /**
     * Level-4 rollup is for 4 hours rollup of raw data
     * @param producedAsMetricRolledUp
     * @param groupedStream
     * @param lateArrivalInMinutes
     */
    private void level4Rollup(Produced<String, RolledUp> producedAsMetricRolledUp,
                              KGroupedStream<String, Metric> groupedStream, long lateArrivalInMinutes)
    {
        long windowSizeLevel4 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel4();
        if(windowSizeLevel4 > 0) {
            rollupForGivenWindowSize(configProps.getTopics().getOutLevel4(),
                    producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel4);
        }
    }

    /**
     * Level-5 rollup is for 1 day rollup of raw data
     * @param producedAsMetricRolledUp
     * @param groupedStream
     * @param lateArrivalInMinutes
     */
    private void level5Rollup(Produced<String, RolledUp> producedAsMetricRolledUp,
                              KGroupedStream<String, Metric> groupedStream, long lateArrivalInMinutes)
    {
        long windowSizeLevel5 = configProps.getStreams().getAggregation().getWindowSizeInMinutesLevel5();
        if(windowSizeLevel5 > 0) {
            rollupForGivenWindowSize(configProps.getTopics().getOutLevel5(),
                    producedAsMetricRolledUp, groupedStream, lateArrivalInMinutes, windowSizeLevel5);
        }
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
            long windowSize, long retentionPeriod, KGroupedStream<String, Metric> groupedStream)
    {
        TimeWindows timeWindows = TimeWindows
                .of(TimeUnit.MINUTES.toMillis(windowSize))
                .until(TimeUnit.MINUTES.toMillis(retentionPeriod));

        // Aggregating with time-based windowing (tumbling windows)
        return groupedStream.windowedBy(timeWindows).reduce((aggValue, newValue) -> aggValue.reduce(newValue));
    }
}
