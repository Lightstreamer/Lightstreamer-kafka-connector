package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class MyDeserializer {

    MyDeserializer(ConnectorConfig config, boolean isKey) {
        Map<String, String> props = new HashMap<>();
        if (config.hasKeySchemaFile() || config.hasValueSchemaFile()) {
            // deserializer = new JsonLocalSchemaDeserializer();
        } else {
            String schemaRegistryUrl = isKey ? config.getHost(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL)
                    : config.getHost(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL);
            if (schemaRegistryUrl != null) {
                props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            }
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, JsonNode.class.getName());
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
            // deserializer = new KafkaJsonSchemaDeserializer<JsonNode>();
        }
        // deserializer.configure(config.extendsonsumerProps(props), isKey);
    }

}
