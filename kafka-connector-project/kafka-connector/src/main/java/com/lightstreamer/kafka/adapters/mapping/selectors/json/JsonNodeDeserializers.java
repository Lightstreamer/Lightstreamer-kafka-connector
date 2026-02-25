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

import com.azure.identity.ClientSecretCredentialBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.everit.json.schema.ValidationException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class JsonNodeDeserializers {

    private static final String SCHEMA_REGISTRY_PROVIDER_AZURE = "AZURE";

    static class JsonNodeLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

        private final ObjectMapper objectMapper = new ObjectMapper();
        private final KafkaJsonDeserializer<JsonNode> deserializer;
        private JsonSchema schema;

        JsonNodeLocalSchemaDeserializer() {
            deserializer = new KafkaJsonDeserializer<>();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
            try {
                schema = new JsonSchema(objectMapper.readTree(getSchemaFile(isKey)));
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            JsonNodeLocalSchemaDeserializer localSchemaDeser =
                    new JsonNodeLocalSchemaDeserializer();
            localSchemaDeser.preConfigure(config, isKey);
            return localSchemaDeser;
        }
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (SCHEMA_REGISTRY_PROVIDER_AZURE.equalsIgnoreCase(config.schemaRegistryProvider())) {
                return azureSchemaRegistryDeserializer();
            }
            return new KafkaJsonSchemaDeserializer<>();
        }
        return new KafkaJsonDeserializer<>();
    }

    private static Deserializer<JsonNode> azureSchemaRegistryDeserializer() {
        return new Deserializer<JsonNode>() {
            private final com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonDeserializer<Object>
                    delegate = new com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonDeserializer<>();
            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                String tenantId = (String) configs.get(SchemaRegistryConfigs.AZURE_TENANT_ID);
                String clientId = (String) configs.get(SchemaRegistryConfigs.AZURE_CLIENT_ID);
                String clientSecret = (String) configs.get(SchemaRegistryConfigs.AZURE_CLIENT_SECRET);

                Map<String, Object> mutableConfigs = new HashMap<>(configs);
                if (tenantId != null && !tenantId.isEmpty()
                        && clientId != null && !clientId.isEmpty()
                        && clientSecret != null && !clientSecret.isEmpty()) {
                    mutableConfigs.put(
                            "schema.registry.credential",
                            new ClientSecretCredentialBuilder()
                                    .tenantId(tenantId)
                                    .clientId(clientId)
                                    .clientSecret(clientSecret)
                                    .build());
                }
                delegate.configure(mutableConfigs, isKey);
            }

            @Override
            public JsonNode deserialize(String topic, byte[] data) {
                return toJsonNode(delegate.deserialize(topic, data));
            }

            @Override
            public JsonNode deserialize(String topic, Headers headers, byte[] data) {
                return toJsonNode(delegate.deserialize(topic, headers, data));
            }

            private JsonNode toJsonNode(Object data) {
                if (data == null) {
                    return null;
                }
                if (data instanceof JsonNode node) {
                    return node;
                }
                return mapper.valueToTree(data);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
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
