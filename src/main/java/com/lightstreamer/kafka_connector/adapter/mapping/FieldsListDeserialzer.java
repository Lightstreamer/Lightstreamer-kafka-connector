package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

interface DataConverter {
    Map<String, String> convert(byte[] data);
}

public class FieldsListDeserialzer implements Deserializer<FieldsList> {

    private DataConverter converter;

    public FieldsListDeserialzer(DataConverter converter) {
        this.converter = converter;
    }

    public FieldsListDeserialzer() {
        this(new SimpleConverter());
    }

    @Override
    public FieldsList deserialize(String topic, byte[] data) {
        return new FieldsList(converter.convert(data));
    }
}
