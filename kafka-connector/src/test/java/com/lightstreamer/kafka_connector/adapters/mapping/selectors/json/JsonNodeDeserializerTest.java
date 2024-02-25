
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class JsonNodeDeserializerTest {

    @Test
    public void shouldDeserializeWithNoSchema() {
        String s = "{\"stock_name\":\"Ations Europe\"}";
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(ConnectorConfig.VALUE_EVALUATOR_TYPE, "JSON"));
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            deser.deserialize("topic", s.getBytes());
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithNoSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                ConnectorConfig.KEY_EVALUATOR_TYPE,
                                "JSON",
                                ConnectorConfig.VALUE_EVALUATOR_TYPE,
                                "JSON"));
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGeKeyDeserializerWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specific settings for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetValueDeserializerWithSchemaRegsitry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.VALUE_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specific settings for the key deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyAndVaueDeserializeWithSchemaRegisstry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.VALUE_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");

        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path keySchemaFile = Files.createTempFile(adapterDir, "key_schema_", "json");
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        keySchemaFile.toFile().getName());
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        // No specific settings for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetValueDeserializerWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path valueSchemaFile = Files.createTempFile(adapterDir, "value_schema_", "json");
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.VALUE_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                        valueSchemaFile.toFile().getName());
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        // No specific settings for the key deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path keySchemaFile = Files.createTempFile(adapterDir, "key_schema_", "json");
        Path valueSchemaFile = Files.createTempFile(adapterDir, "value_schema_", "json");
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        keySchemaFile.toFile().getName(),
                        ConnectorConfig.VALUE_EVALUATOR_TYPE,
                        "JSON",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                        valueSchemaFile.toFile().getName());
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }
    }
}
