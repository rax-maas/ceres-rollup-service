package com.rackspacecloud.metrics.rollup.config;

import com.rackspacecloud.metrics.rollup.domain.Metric;
import com.rackspacecloud.metrics.rollup.domain.RolledUp;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJODeserializer;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJOSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(KafkaConfigurationProperties.class)
public class KafkaStreamsConfiguration {
    KafkaConfigurationProperties configProps;

    @Autowired
    public KafkaStreamsConfiguration(KafkaConfigurationProperties kafkaConfigurationProperties){
        this.configProps = kafkaConfigurationProperties;
    }

    private Properties getStreamsConfiguration() {
        Properties props = new Properties();
        setStreamsConfig(props);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configProps.getConsumer().getAutoOffsetResetConfig());

        setSslProperties(props);

//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        return props;
    }

    private void setStreamsConfig(Properties props) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, configProps.getStreams().getApplicationIdConfig());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configProps.getServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, FailOnInvalidTimestamp.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, configProps.getStreams().getStateDirConfig());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());
    }

    private void setSslProperties(Properties props) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configProps.getProperties().getSecurityProtocol());

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configProps.getSsl().getTruststoreLocation());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, configProps.getSsl().getTruststorePassword());
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configProps.getSsl().getKeystoreLocation());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configProps.getSsl().getKeystorePassword());
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, configProps.getSsl().getKeyPassword());
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Consumed<String, Metric> consumedAsMetric = getStringMetricConsumed();
        final Produced<String, RolledUp> producedAsMetricRolledUp = getStringRolledUpProduced();

        final KStream<String, Metric> source = builder.stream(configProps.getTopics().getIn(), consumedAsMetric);

        KStream<String, Metric> rollupPrepped = source.map(
                (key, metric) -> KeyValue.pair(metric.getRollupKey(), metric)
        );

        KGroupedStream<String, Metric> groupedStream = rollupPrepped.groupByKey(
                Serialized.with(Serdes.String(), getMetricSerde())
        );

        KTable<Windowed<String>, Metric> timeWindowedAggregatedStream = getWindowedMetricKTable(groupedStream);

        KStream<String, RolledUp> rolledUpData = timeWindowedAggregatedStream.toStream()
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(),
                        new RolledUp(windowedKey.key(), windowedKey.window().start(),
                                windowedKey.window().end(), value)));

        Topology topology = builder.build();
//        System.out.println(topology.describe());

//        rolledUpData.foreach((key, value) -> {
//            System.out.println(String.format("key=%s;start=%s;end=%s", value.key, value.start, value.end));
//            for(String key2 : value.ivalues.keySet()){
//                System.out.println(String.format("%s=%s", key2, value.ivalues.get(key2)));
//            }
//        });

        rolledUpData.to(configProps.getTopics().getOut(), producedAsMetricRolledUp);

        final KafkaStreams streams = new KafkaStreams(topology, getStreamsConfiguration());

        return streams;
    }

    private KTable<Windowed<String>, Metric> getWindowedMetricKTable(KGroupedStream<String, Metric> groupedStream) {
        long retentionPeriod = configProps.getStreams().getAggregation().getWindowRetentionPeriodInMinutes();
        long windowSize = configProps.getStreams().getAggregation().getWindowSizeInMinutes();

//        TimeWindows timeWindows = TimeWindows
//                .of(TimeUnit.MINUTES.toMillis(windowSize))
//                .until(TimeUnit.MINUTES.toMillis(retentionPeriod));

        TimeWindows timeWindows = TimeWindows
                .of(TimeUnit.SECONDS.toMillis(10));
//                .until(TimeUnit.MINUTES.toMillis(retentionPeriod));

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
