package com.lightstreamer.kafka_connector.adapter.consumers.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class GenericRecordValueSelectorSupplier extends AbstractSelectorSupplier<GenericRecord>
        implements ValueSelectorSupplier<GenericRecord> {

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }

    @Override
    public ValueSelector<GenericRecord> selector(String name, String expression) {
        return new GenericRecordValueSelector(name, expression);
    }

    @Override
    protected Class<?> getLocalSchemaDeserializer() {
        return AvroLocalSchemaDeserializer.class;
    }

    @Override
    protected Class<?> getSchemaDeserializer() {
        return KafkaAvroDeserializer.class;
    }
}
