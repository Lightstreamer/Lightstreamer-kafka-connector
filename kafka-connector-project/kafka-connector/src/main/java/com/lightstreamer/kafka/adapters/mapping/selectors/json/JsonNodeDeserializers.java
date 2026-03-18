
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SchemaRegistryProvider.AZURE;

import com.azure.data.schemaregistry.SchemaRegistryAsyncClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractAzureSchemaRegistryDeserializer;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class JsonNodeDeserializers {

    static class JsonNodeAzureSchemaRegistryDeserializer
            extends AbstractAzureSchemaRegistryDeserializer<JsonNode> {

        private final Map<String, JsonSchema> schemaCache = new HashMap<>();
        private ObjectMapper mapper;
        private SchemaRegistryAsyncClient client;

        JsonNodeAzureSchemaRegistryDeserializer(ConnectorConfig config) {
            super(config);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.client =
                    new SchemaRegistryClientBuilder()
                            .fullyQualifiedNamespace(getConfig().schemaRegistryUrl())
                            .credential(getTokenCredential())
                            .buildAsyncClient();

            this.mapper =
                    new ObjectMapper()
                            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public JsonNode deserialize(String topic, byte[] data) {
            throw new UnsupportedOperationException(
                    "AzureSchemaRegistryDeserializer requires Kafka record headers; use"
                            + " deserialize(topic, headers, data) instead");
        }

        /**
         * Deserializes the JSON value from the given byte array, validating it against the schema
         * identified by the {@code schemaId} header.
         *
         * @param topic the topic name
         * @param headers the Kafka record headers; must contain a {@code schemaId} header
         * @param data the raw bytes to deserialize; returns {@code null} if null or empty
         * @return the deserialized {@link JsonNode}
         * @throws SerializationException if the {@code schemaId} header is missing, the schema
         *     cannot be retrieved from Azure Schema Registry, the schema definition is null, or the
         *     payload fails schema validation
         */
        @Override
        public JsonNode deserialize(String topic, Headers headers, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }

            try {
                JsonNode node = mapper.readValue(data, JsonNode.class);
                String schemaId = extractSchemaId(headers);
                getSchema(schemaId).validate(node);
                return node;
            } catch (IOException | ValidationException e) {
                throw new SerializationException(e.getMessage(), e);
            }
        }

        private String extractSchemaId(Headers headers) {
            var header = headers.lastHeader("schemaId");
            if (header == null) {
                throw new SerializationException("Schema Id was not found in record headers", null);
            }
            return new String(header.value(), StandardCharsets.UTF_8);
        }

        private JsonSchema getSchema(String schemaId) {
            JsonSchema cached = schemaCache.get(schemaId);
            if (cached != null) {
                return cached;
            }
            SchemaRegistrySchema registrySchema;
            try {
                // SchemaRegistryAsyncClient is used instead of the sync SchemaRegistryClient
                // because the sync client's getSchema() calls block() with no timeout, which
                // can hang indefinitely when the schema registry URL is wrong or unreachable —
                // transport-level timeouts (connect/response) are insufficient because the
                // Netty HTTP client may stall before they apply (e.g. during TLS handshake).
                // Applying Reactor's timeout() operator on the Mono before block() guarantees
                // the call is always bounded, regardless of the transport state.
                registrySchema = client.getSchema(schemaId).timeout(Duration.ofSeconds(2)).block();
            } catch (RuntimeException e) {
                throw new SerializationException(
                        "Failed to retrieve schema for id: " + schemaId, e);
            }
            String definition = registrySchema.getDefinition();
            if (definition == null) {
                throw new SerializationException(
                        "Schema definition is null for schema id: " + schemaId, null);
            }
            JsonSchema schema = new JsonSchema(definition);
            schemaCache.put(schemaId, schema);
            return schema;
        }
    }

    static class JsonNodeLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

        private final ObjectMapper objectMapper = new ObjectMapper();
        private final KafkaJsonDeserializer<JsonNode> delegate = new KafkaJsonDeserializer<>();
        private JsonSchema schema;

        JsonNodeLocalSchemaDeserializer(ConnectorConfig config) {
            super(config);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            delegate.configure(configs, isKey);
            try {
                schema = new JsonSchema(objectMapper.readTree(getSchemaFile(isKey)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JsonNode deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                JsonNode node = delegate.deserialize(topic, data);
                schema.validate(node);
                return node;
            } catch (IOException | ValidationException e) {
                throw new SerializationException(e.getMessage());
            }
        }
    }

    public static Deserializer<JsonNode> ValueDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, false);
    }

    public static Deserializer<JsonNode> KeyDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, true);
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
            return new JsonNodeLocalSchemaDeserializer(config);
        }
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (AZURE.equals(config.schemaRegistryProvider())) {
                return new JsonNodeAzureSchemaRegistryDeserializer(config);
            }
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
