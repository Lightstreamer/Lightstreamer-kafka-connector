
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.adapters.mapping.selectors.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.everit.json.schema.ValidationException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    private Deserializer<JsonNode> deserializer;

    private final boolean isKey;

    JsonNodeDeserializer(ConnectorConfig config, boolean isKey) {
        this.isKey = isKey;
        if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
            deserializer = new JsonLocalSchemaDeserializer(config, isKey);
        } else {
            if (isKey) {
                if (config.isSchemaRegistryEnabledForKey()) {
                    deserializer = new KafkaJsonSchemaDeserializer<JsonNode>();
                }
            } else {
                if (config.isSchemaRegistryEnabledForValue()) {
                    deserializer = new KafkaJsonSchemaDeserializer<JsonNode>();
                }
            }
        }
        if (deserializer == null) {
            deserializer = new KafkaJsonDeserializer<>();
        }

        deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
    }

    JsonNodeDeserializer(boolean isKey) {
        this.isKey = isKey;
        deserializer = new KafkaJsonDeserializer<>();
        deserializer.configure(Collections.emptyMap(), isKey);
    }

    public boolean isKey() {
        return isKey;
    }

    public String deserializerClassName() {
        return deserializer.getClass().getName();
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        return deserializer.deserialize(topic, data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}

class JsonLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KafkaJsonDeserializer<JsonNode> deserializer;

    private JsonSchema schema;

    public JsonLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
        super(config, isKey);
        deserializer = new KafkaJsonDeserializer<>();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
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
