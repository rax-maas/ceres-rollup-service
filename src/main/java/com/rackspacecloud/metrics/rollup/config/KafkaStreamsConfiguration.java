package com.rackspacecloud.metrics.rollup.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Properties;

/**
 * This class configures Kafka streams with whatever configuration needs.
 */
@Configuration
@EnableConfigurationProperties(KafkaConfigurationProperties.class)
public class KafkaStreamsConfiguration {
    final KafkaConfigurationProperties configProps;
    final Properties config;
    final Topology topology;

    @Autowired
    public KafkaStreamsConfiguration(KafkaConfigurationProperties kafkaConfigurationProperties, Topology topology){
        this.configProps = kafkaConfigurationProperties;
        this.config = new Properties();
        this.topology = topology;
    }

    @Bean
    @Profile("development")
    KafkaStreams devKafkaStreams() {
        setStreamsConfiguration(false);
        final KafkaStreams streams = new KafkaStreams(topology, config);

        return streams;
    }

    @Bean
    @Profile("production")
    KafkaStreams prodKafkaStreams() {
        setStreamsConfiguration(true);
        final KafkaStreams streams = new KafkaStreams(topology, config);

        return streams;
    }

    private void setSslProperties() {
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configProps.getProperties().getSecurityProtocol());

        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configProps.getSsl().getTruststoreLocation());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, configProps.getSsl().getTruststorePassword());
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configProps.getSsl().getKeystoreLocation());
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configProps.getSsl().getKeystorePassword());
        config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, configProps.getSsl().getKeyPassword());
        config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }

    private void setStreamsConfiguration(boolean setSslProperties) {
        setStreamsConfig();
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configProps.getConsumer().getAutoOffsetResetConfig());

//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configProps.getServers());
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        if(setSslProperties) setSslProperties();
    }

    private void setStreamsConfig() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, configProps.getStreams().getApplicationIdConfig());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configProps.getServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, FailOnInvalidTimestamp.class.getName());
        config.put(StreamsConfig.STATE_DIR_CONFIG, configProps.getStreams().getStateDirConfig());
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());
    }
}
