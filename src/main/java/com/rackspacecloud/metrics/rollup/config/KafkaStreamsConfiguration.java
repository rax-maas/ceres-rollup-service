package com.rackspacecloud.metrics.rollup.config;

import com.rackspacecloud.metrics.rollup.domain.Metric;
import com.rackspacecloud.metrics.rollup.domain.RolledUp;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJODeserializer;
import com.rackspacecloud.metrics.rollup.serdes.JsonPOJOSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Configuration
public class KafkaStreamsConfiguration {

    @Bean
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());

        return new StreamsConfig(props);
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Consumed<String, Metric> consumedAsMetric = getStringMetricConsumed();
        final Produced<String, RolledUp> producedAsMetricRolledUp = getStringRolledUpProduced();

        final KStream<String, Metric> input = builder.stream("unified.metrics.json", consumedAsMetric);

        KStream<String, Metric> rollupPrepped = input.map(
                (key, metric) -> KeyValue.pair(metric.getRollupKey(), metric)
        );

        KGroupedStream<String, Metric> groupedStream = rollupPrepped.groupByKey(
                Serialized.with(Serdes.String(), getMetricSerde())
        );

        // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
        // TODO: timing window is altered. Still working on it.
        KTable<Windowed<String>, Metric> timeWindowedAggregatedStream = groupedStream.windowedBy(
                TimeWindows.of(TimeUnit.SECONDS.toMillis(10)) /* time-based window */)
                .reduce((aggValue, newValue) -> aggValue.reduce(newValue));

        KStream<String, RolledUp> rolledUpData = timeWindowedAggregatedStream.toStream()
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(),
                        new RolledUp(windowedKey.key(), windowedKey.window().start(),
                                windowedKey.window().end(), value)));

        rolledUpData.foreach((key, value) -> {
            System.out.println(String.format("key=%s;start=%s;end=%s", value.key, value.start, value.end));
            for(String key2 : value.ivalues.keySet()){
                System.out.println(String.format("%s=%s", key2, value.ivalues.get(key2)));
            }
        });

//        timeWindowedAggregatedStream.toStream()
//                .foreach(new ForeachAction<Windowed<String>, Metric.Values>() {
//                    @Override
//                    public void apply(Windowed<String> stringWindowed, Metric.Values values) {
//                        System.out.println(String.format("key=%s", stringWindowed.key()));
//                        System.out.println(String.format("start=%s", stringWindowed.window().start()));
//                        System.out.println(String.format("end=%s", stringWindowed.window().end()));
//
//                        for(String key : values.ivalues.keySet()){
//                            System.out.println(String.format("%s=%s", key, values.ivalues.get(key)));
//                        }
//                    }
//                });

        rolledUpData.to("mrit-stream-output-topic", producedAsMetricRolledUp);


        final KafkaStreams streams = new KafkaStreams(builder.build(), kStreamsConfigs());

        return streams;
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
