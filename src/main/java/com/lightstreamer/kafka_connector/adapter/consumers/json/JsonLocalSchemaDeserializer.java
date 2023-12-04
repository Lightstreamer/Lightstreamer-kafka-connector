package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.everit.json.schema.ValidationException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

public class JsonLocalSchemaDeserializer implements Deserializer<JsonNode> {

    private ObjectMapper objectMapper = new ObjectMapper();

    private JsonSchema schema;

    public JsonLocalSchemaDeserializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        schema = getFileSchema(isKey ? "key.schema.file" : "value.schema.file", configs);
    }

    private JsonSchema getFileSchema(String setting, Map<String, ?> configs) {
        Object fileSchema = configs.get(setting);
        if (fileSchema == null) {
            throw new SerializationException(setting + " setting is mandatory");
        }
        if (fileSchema instanceof String f) {
            try {
                File file = new File(f);
                System.out.println(file.getAbsolutePath());
                return new JsonSchema(objectMapper.readTree(file));
            } catch (IOException e) {
                throw new SerializationException(e.getMessage());
            }
        }
        throw new SerializationException("Unable to load schema file " + fileSchema);
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);
            schema.validate(node);
            return node;
        } catch (IOException | ValidationException e) {
            throw new SerializationException(e.getMessage());
        }
    }
}
