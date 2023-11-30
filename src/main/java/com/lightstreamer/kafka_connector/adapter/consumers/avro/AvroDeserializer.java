package com.lightstreamer.kafka_connector.adapter.consumers.avro;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroDeserializer implements Deserializer<GenericRecord> {

    private Schema schema;

    public AvroDeserializer() {

    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        schema = getFileSchema(isKey ? "key.schema.file" : "value.schema.file", configs);
    }

    private Schema getFileSchema(String setting, Map<String, ?> configs) {
        Object fileSchema = configs.get(setting);
        if (fileSchema == null) {
            throw new SerializationException(setting + " setting is mandatory");
        }
        if (fileSchema instanceof String f) {
            try {
                return new Schema.Parser().parse(new File(f));
            } catch (IOException e) {
                throw new SerializationException(e.getMessage());
            }
        }
        throw new SerializationException();
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data, null);
        try {
            return datumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
