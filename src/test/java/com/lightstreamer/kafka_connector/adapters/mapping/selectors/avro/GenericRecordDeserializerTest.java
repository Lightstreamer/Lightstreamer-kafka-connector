
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka_connector.adapters.config.ConfigException;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class GenericRecordDeserializerTest {

    @Test
    public void shouldNotDeserializeWithoutSchemaSettings() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        ConfigException e1 =
                assertThrows(
                        ConfigException.class, () -> new GenericRecordDeserializer(config, true));
        assertThat(e1.getMessage())
                .isEqualTo("Missing required parameter [key.evaluator.schema.registry.url]");
        ConfigException e2 =
                assertThrows(
                        ConfigException.class, () -> new GenericRecordDeserializer(config, false));
        assertThat(e2.getMessage())
                .isEqualTo("Missing required parameter [value.evaluator.schema.registry.url]");
    }

    @Test
    public void shouldDeserializeWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL,
                        "http://host-key:8080",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL,
                        "http://host-value:8080");
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
    public void shouldDeserializeKeyWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH, "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeValueWithLocalSchema() throws IOException {
        Map<String, String> otherConfigs =
                Map.of(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH, "value.avsc");
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith("src/test/resources", otherConfigs);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }
}
