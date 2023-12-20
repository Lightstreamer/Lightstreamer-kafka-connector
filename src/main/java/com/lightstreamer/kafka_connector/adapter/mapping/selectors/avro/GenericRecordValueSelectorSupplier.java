package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class GenericRecordValueSelectorSupplier extends AbstractSelectorSupplier<GenericRecord>
        implements ValueSelectorSupplier<GenericRecord> {

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }

    @Override
    protected Class<?> getLocalSchemaDeserializer() {
        return GenericRecordLocalSchemaDeserializer.class;
    }

    @Override
    protected Class<?> getSchemaDeserializer() {
        return KafkaAvroDeserializer.class;
    }

    @Override
    public String deserializer(Properties props) {
        return super.deserializer(false, props);
    }

    @Override
    public ValueSelector<GenericRecord> selector(String name, String expression) {
        return new GenericRecordValueSelector(name, expectedRoot(), expression);
    }
}
