
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.JSON;

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

class JsonNodeDeserializers {

    static class JsonNodeLocalDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

        private final ObjectMapper objectMapper = new ObjectMapper();
        private final KafkaJsonDeserializer<JsonNode> deserializer;
        private JsonSchema schema;

        JsonNodeLocalDeserializer() {
            deserializer = new KafkaJsonDeserializer<>();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
            try {
                schema = new JsonSchema(objectMapper.readTree(getSchemaFile(isKey)));
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

    static Deserializer<JsonNode> ValueDeserializer() {
        return makeDeserializerNoConfig(false);
    }

    static Deserializer<JsonNode> ValueDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, false);
    }

    static Deserializer<JsonNode> KeyDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, true);
    }

    static Deserializer<JsonNode> KeyDeserializer() {
        return makeDeserializerNoConfig(true);
    }

    private static Deserializer<JsonNode> makeDeserializerNoConfig(boolean isKey) {
        Deserializer<JsonNode> deserializer = new KafkaJsonDeserializer<>();
        deserializer.configure(Collections.emptyMap(), isKey);
        return deserializer;
    }

    private static Deserializer<JsonNode> configuredDeserializer(
            ConnectorConfig config, boolean isKey) {
        checkEvaluator(config, isKey);
        Deserializer<JsonNode> deserializer = newDeserializer(config, isKey);
        deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
        return deserializer;
    }

    private static Deserializer<JsonNode> newDeserializer(ConnectorConfig config, boolean isKey) {
        if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
            JsonNodeLocalDeserializer jsonNodeLocalDeserializer = new JsonNodeLocalDeserializer();
            jsonNodeLocalDeserializer.preConfigure(config);
            return jsonNodeLocalDeserializer;
        }
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            return new KafkaJsonSchemaDeserializer<>();
        }
        return new KafkaJsonDeserializer<>();
    }

    private static void checkEvaluator(ConnectorConfig config, boolean isKey) {
        if (isJsonKeyEvaluator(config, isKey)) {
            return;
        }
        if (isJsonValueEvaluator(config, isKey)) {
            return;
        }
        throw new IllegalArgumentException("Evaluator type is not JSON");
    }

    private static boolean isJsonKeyEvaluator(ConnectorConfig config, boolean isKey) {
        if (!isKey) {
            return false;
        }
        return config.getKeyEvaluator().is(JSON);
    }

    private static boolean isJsonValueEvaluator(ConnectorConfig config, boolean isKey) {
        if (isKey) {
            return false;
        }
        return config.getValueEvaluator().is(JSON);
    }
}
