
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

package com.lightstreamer.kafka.adapters.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializer.GenericRecordLocalSchemaDeserializer;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializer.WrapperKafkaAvroDeserializer;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class GenericRecordDeserializerTest {

    @Test
    public void shouldDeserializeWithLocalSchema() {
        Path adapterDir = Paths.get("src/test/resources");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                "value.avsc"));

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
        try (Deserializer<GenericRecord> deserializer = GenericRecordDeserializer.ValueDeserializer(config)) {
            GenericRecord node = deserializer.deserialize("topic", flight.getBytes());
        //     assertThat(node.get("airline").asText()).isEqualTo("Lightstreamer Airlines");
        //     assertThat(node.get("code").asText()).isEqualTo("LA1704");
        //     assertThat(node.get("departureTime").asText()).isEqualTo("19:25");
        //     assertThat(node.get("status").asText()).isEqualTo("SCHEDULED_ON_TIME");
        //     assertThat(node.get("terminal").asInt()).isEqualTo(1);
        }
    }        

    @Test
    public void shouldGetKeyDeserializerWithSchemaRegsitry() {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deserializer.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetValueDeserializerWithSchemaRegsitry() {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deserializer.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyAndVaueDeserializeWithSchemaRegisstry() {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deserializer.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetValueDeserializerWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deserializer.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldGetKeyDeserializeWithSchemaRegistryValueDeserializerWithLocalSchema() {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchemaValueWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(WrapperKafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializazionWithLocalSchemaTakesPrecedenceOverSchemaRegistry()
            throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080",
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.KeyDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializer.ValueDeserializer(config)) {
            assertThat(deser.getClass().getName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }
}
