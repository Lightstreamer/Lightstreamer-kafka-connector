package com.lightstreamer.kafka_connector.adapter.consumers.avro;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplierWithSchema;
import com.lightstreamer.kafka_connector.adapter.evaluator.Selector;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class GenericRecordSelectorSupplier extends AbstractSelectorSupplierWithSchema<GenericRecord> {

    public GenericRecordSelectorSupplier(Map<String, String> conf, boolean isKey) {
        if (isKey) {
            configKeyDeserializer(conf);
        } else {
            configValueDeserializer(conf);
        }
    }

    @Override
    public Selector<GenericRecord> get(String name, String expression) {
        return new GenericRecordSelector(name, expression);
    }

    @Override
    protected Class<?> getLocalSchemaDeserializer() {
        return AvroDeserializer.class;
    }

    @Override
    protected Class<?> getSchemaDeserializer() {
        return KafkaAvroDeserializer.class;
    }
}
