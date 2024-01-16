package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.everit.json.schema.ValidationException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    private Deserializer<JsonNode> deserializer;

    JsonNodeDeserializer(ConnectorConfig config, boolean isKey) {
        Map<String, String> props = new HashMap<>();
        if (config.hasKeySchemaFile() || config.hasValueSchemaFile()) {
            deserializer = new JsonLocalSchemaDeserializer();
        } else {
            deserializer = new KafkaJsonSchemaDeserializer<JsonNode>();
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, JsonNode.class.getName());
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        }
        deserializer.configure(config.extendsonsumerProps(props), isKey);
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        return deserializer.deserialize(topic, data);
    }

}

class JsonLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KafkaJsonDeserializer<JsonNode> deserializer;

    private JsonSchema schema;

    public JsonLocalSchemaDeserializer() {
        deserializer = new KafkaJsonDeserializer<>();
    }

    @Override
    protected void doConfigure(Map<String, ?> configs, File schemaFile, boolean isKey) {
        deserializer.configure(configs, isKey);
        try {
            schema = new JsonSchema(objectMapper.readTree(schemaFile));
        } catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        try {
            JsonNode node = deserializer.deserialize(topic, data);
            schema.validate(node);
            return node;
        } catch (IOException | ValidationException e) {
            throw new SerializationException(e.getMessage());
        }
    }
}
