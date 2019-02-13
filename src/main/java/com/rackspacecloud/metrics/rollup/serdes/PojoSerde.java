package com.rackspacecloud.metrics.rollup.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Map;

/**
 * This class creates Serializer and Deserializer for the given POJO type.
 * @param <T> POJO type for the serializer and deserializer.
 */
public class PojoSerde<T> implements Serde<T> {
    Class<T> pojoType;
    Serializer<T> pojoSerializer;
    Deserializer<T> pojoDeserializer;

    public PojoSerde(){
        throw new NotImplementedException();
    }

    public PojoSerde(Class<T> pojoType) {
        this.pojoType = pojoType;
        this.pojoSerializer = new PojoSerializer<>();
        this.pojoDeserializer = new PojoDeserializer<>(pojoType);
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

    static class PojoSerializer<T> implements Serializer<T> {

        @Override
        public void configure(Map<String, ?> map, boolean isKey) {

        }

        /**
         * Serialize data into the given topic.
         * @param topic topic name
         * @param data data to send to the given topic
         * @return
         */
        @Override
        public byte[] serialize(String topic, T data) {
            ObjectMapper mapper = new ObjectMapper();

            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                String typeName = data.getClass().getName();
                throw new SerializationException("Error serializing JSON message of type [" + typeName + "]" +
                        "into topic [" + topic + "]", e);
            }
        }

        @Override
        public void close() {

        }
    }

    static class PojoDeserializer<T> implements Deserializer<T> {
        Class<T> pojoType;

        PojoDeserializer(){
            throw new NotImplementedException();
        }

        PojoDeserializer(Class<T> pojoType) {
            this.pojoType = pojoType;
        }

        @Override
        public void configure(Map<String, ?> map, boolean isKey) {
        }

        /**
         * Deserialize data from a given topic
         * @param topic topic name
         * @param data data to get from the given topic
         * @return
         */
        @Override
        public T deserialize(String topic, byte[] data) {
            ObjectMapper mapper = new ObjectMapper();

            try {
                return mapper.readValue(data, pojoType);
            } catch (IOException e) {
                System.out.println(String.format("topic = [%s]", topic));
                throw new SerializationException(e);
            }
        }

        @Override
        public void close() {

        }
    }
}
