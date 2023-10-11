package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Map;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordEvaluator;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class SymbolConsumerLoop extends AbstractConsumerLoop<GenericEnumSymbol<?>> {

    private static class GenericEnumSymbolEvaluator implements RecordEvaluator<GenericEnumSymbol<?>> {

        private String schema;

        GenericEnumSymbolEvaluator(String schema) {
            this.schema = schema;
        }

        @Override
        public String extract(GenericEnumSymbol<?> value) {
            if (!value.getSchema().getName().equals(schema)) {
                log.warn("Message is not of type {}", schema);
            }
            return value.toString();
        }

    }

    public SymbolConsumerLoop(Map<String, String> configuration, TopicMapping item, ItemEventListener eventListener) {
        super(configuration, item, GenericEnumSymbolEvaluator::new, eventListener);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // properties.put("schema.registry.url", "http://schema-registry:8081");
    }
}
