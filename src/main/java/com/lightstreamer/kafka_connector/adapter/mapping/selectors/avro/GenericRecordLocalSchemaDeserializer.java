package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

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

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractLocalSchemaDeserializer;

public class GenericRecordLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<GenericRecord> {

    private Schema schema;

    public GenericRecordLocalSchemaDeserializer() {

    }

    @Override
    protected void doConfigure(Map<String, ?> configs, File schemaFile, boolean isKey) {
        try {
            schema = new Schema.Parser().parse(schemaFile);
        } catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
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
