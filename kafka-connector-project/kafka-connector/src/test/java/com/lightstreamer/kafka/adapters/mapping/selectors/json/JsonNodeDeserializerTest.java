
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

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.JSON;

import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeDeserializers.JsonNodeLocalSchemaDeserializer;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class JsonNodeDeserializerTest {

    private static final String SCHEMA_FOLDER = "src/test/resources";
    private static final String TEST_SCHEMA_FILE = "flights.json";

    @Test
    public void shouldDeserializeWithNoSchema() {
        String s = "{\"stock_name\":\"Ations Europe\"}";
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, JSON.toString()));
        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.ValueDeserializer(config)) {
            deser.deserialize("topic", s.getBytes());
        }
    }

    @Test
    public void shouldDeserializeWithLocalSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        String flight =
                """
          {
            "airline": "Lightstreamer Airlines",
            "code": "LA1704",
            "departureTime": "19:25",
            "status": "SCHEDULED_ON_TIME",
            "terminal": 1
          }
                """;
        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            JsonNode node = deserializer.deserialize("topic", flight.getBytes());
            assertThat(node.get("airline").asText()).isEqualTo("Lightstreamer Airlines");
            assertThat(node.get("code").asText()).isEqualTo("LA1704");
            assertThat(node.get("departureTime").asText()).isEqualTo("19:25");
            assertThat(node.get("status").asText()).isEqualTo("SCHEDULED_ON_TIME");
            assertThat(node.get("terminal").asInt()).isEqualTo(1);
        }
    }

    @Test
    public void shouldNotDeserializeWithLocalSchemaDueToInvalidRecord() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        // Missing required 'terminal' key
        String flight =
                """
          {
            "airline": "Lightstreamer Airlines",
            "code": "LA1704",
            "departureTime": "2025-01-01:12.05.00",
            "status": "SCHEDULED_ON_TIME"
          }
                """;
        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.ValueDeserializer(config)) {
            SerializationException se =
                    assertThrows(
                            SerializationException.class,
                            () -> deser.deserialize("topic", flight.getBytes()));
            assertThat(se.getMessage()).contains("required key [terminal] not found");
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithNoSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString()));
        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonDeserializer.class);
        }

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonDeserializer.class);
        }
    }

    @Test
    public void shouldGeKeyDeserializerWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetValueDeserializerWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializeWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetValueDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyDeserializeWithSchemaRegistryValueDeserializerWithLocalSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }

        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchemaValueWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }

        try (Deserializer<JsonNode> deser = JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaJsonSchemaDeserializer.class);
        }
    }

    @Test
    public void shouldDeserializationWithLocalSchemaTakePrecedenceOverSchemaRegistry()
            throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                JSON.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080",
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<JsonNode> deserializer = JsonNodeDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }

        try (Deserializer<JsonNode> deserializer =
                JsonNodeDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(JsonNodeLocalSchemaDeserializer.class);
        }
    }
}
