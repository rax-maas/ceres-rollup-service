package com.rackspacecloud.metrics.rollup.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSerializer.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topicName, T data) {
        if(data == null) return null;
        LOGGER.debug("Data is [{}]", data);

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Schema schema = data.getSchema();
            Encoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);

            datumWriter.write(data, encoder);
            encoder.flush();
            outputStream.close();

            byte[] result = outputStream.toByteArray();
            LOGGER.debug("serialized data='{}'", DatatypeConverter.printHexBinary(result));

            return result;
        }
        catch (Exception e){
            String errorMessage = String.format("Serialization failed for topic [%s] with exception message: [%s]",
                    topicName, e.getMessage());
            LOGGER.error("{} Data in question is [{}]", errorMessage, data);
            throw new SerializationException(errorMessage, e);
        }
    }

    @Override
    public void close() {

    }
}
