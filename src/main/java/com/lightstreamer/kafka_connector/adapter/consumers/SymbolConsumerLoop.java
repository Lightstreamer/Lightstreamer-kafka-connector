package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Map;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class SymbolConsumerLoop extends AbstractConsumerLoop<GenericEnumSymbol<?>> {

    private static class GenericEnumSymbolEvaluator implements ValueSelector<GenericEnumSymbol<?>> {

        private String schema;

        private String name;

        GenericEnumSymbolEvaluator(String name, String schema) {
            this.name = name;
            this.schema = schema;
        }

        @Override
        public Value extract(GenericEnumSymbol<?> value) {
            if (!value.getSchema().getName().equals(schema)) {
                log.warn("Message is not of type {}", schema);
            }
            return new SimpleValue(name(), value.toString());
        }
        

        @Override
        public String name() {
            return name;
        }

        @Override
        public String expression() {
            return schema;
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
