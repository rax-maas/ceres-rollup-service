package com.rackspacecloud.metrics.rollup.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * This class creates Serializer and Deserializer for the given POJO type.
 * @param <T> POJO type for the serializer and deserializer.
 */
public class PojoSerde<T> implements Serde<T> {
    Class<T> pojoType;
    Serializer<T> pojoSerializer;
    Deserializer<T> pojoDeserializer;

    public PojoSerde(Class<T> pojoType) {
        this.pojoType = pojoType;
        this.pojoSerializer = new JsonSerializer<>();
        this.pojoDeserializer = new JsonDeserializer<>(pojoType);
    }

    @Override
    public void configure(Map<String, ?> map, boolean isKey) {
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return this.pojoSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.pojoDeserializer;
    }
}
