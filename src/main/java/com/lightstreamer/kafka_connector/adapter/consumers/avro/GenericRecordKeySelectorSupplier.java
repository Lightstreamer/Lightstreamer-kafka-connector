package com.lightstreamer.kafka_connector.adapter.consumers.avro;

import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class GenericRecordKeySelectorSupplier extends AbstractSelectorSupplier<GenericRecord>
        implements KeySelectorSupplier<GenericRecord> {

    static final class GenericRecordKeySelector extends GenericRecordBaseSelector
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<GenericRecord, ?> record) {
            return super.eval(record.key());
        }
    }

    @Override
    public KeySelector<GenericRecord> selector(String name, String expression) {
        return new GenericRecordKeySelector(name, expression);
    }

    @Override
    public void configKey(Map<String, String> conf, Properties props) {
        KeySelectorSupplier.super.configKey(conf, props);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_KEY_TYPE_CONFIG, true);
    }

    @Override
    protected Class<?> getLocalSchemaDeserializer() {
        return GenericRecordLocalSchemaDeserializer.class;
    }

    @Override
    protected Class<?> getSchemaDeserializer() {
        return KafkaAvroDeserializer.class;
    }

}
