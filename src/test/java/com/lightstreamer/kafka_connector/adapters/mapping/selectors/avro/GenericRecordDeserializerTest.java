
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class GenericRecordDeserializerTest {

    @Test
    public void shouldDeserializeWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaAvroDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithSchemaRegistryValueWithLocalSchema() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaAvroDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchemaValueWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(KafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializazionWithLocalSchemaTakePrecedenceOverSchemaRegistry()
            throws IOException {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080",
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }
}
