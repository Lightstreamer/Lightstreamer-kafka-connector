package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.interfaces.data.ItemEventListener;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;

public class JsonNodeConsumer extends AbstractConsumerLoop<JsonNode> {

    public JsonNodeConsumer(Map<String, String> configuration, TopicMapping mapping, ItemEventListener eventListener) {
        super(configuration, mapping, JsonNodeEvaluator::new, eventListener);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("schema.registry.url", "http://schema-registry:8081");

    }
}
