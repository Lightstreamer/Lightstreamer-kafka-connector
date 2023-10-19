package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.interfaces.data.ItemEventListener;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;

public class JsonNodeConsumer extends AbstractConsumerLoop<JsonNode> {

    public JsonNodeConsumer(Map<String, String> configuration, List<TopicMapping> mapping, ItemEventListener eventListener) {
        super(configuration, mapping, JsonNodeSelector::new, eventListener);
        // properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("schema.registry.url", "http://schema-registry:8081");
    }
}
