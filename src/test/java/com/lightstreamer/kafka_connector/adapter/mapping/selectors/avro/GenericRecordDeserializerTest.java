
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

package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class GenericRecordDeserializerTest {

    @Test
    public void shouldNotDeserializeWithoutSchemaSettings() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThrows(ConfigException.class, () -> new GenericRecordDeserializer(config, true));
        assertThrows(ConfigException.class, () -> new GenericRecordDeserializer(config, false));
    }

    @Test
    public void shouldDeserializeWithSchemaRegistry() {
        Map<String, String> otherConfigs =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://host-key:8080",
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
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldNotDeserializeKeyDueToMissingLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "no-file.avsc");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        SerializationException e =
                assertThrows(
                        SerializationException.class,
                        () -> new GenericRecordDeserializer(config, true));
        assertThat(e.getMessage())
                .isEqualTo("File [" + adapterDir.toAbsolutePath() + "/no-file.avsc] not found");
    }

    @Test
    public void shouldNotDeserializeValueDueToMissingLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs =
                Map.of(ConnectorConfig.VALUE_SCHEMA_FILE, "no-file.avsc");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        SerializationException e =
                assertThrows(
                        SerializationException.class,
                        () -> new GenericRecordDeserializer(config, false));
        assertThat(e.getMessage())
                .isEqualTo("File [" + adapterDir.toAbsolutePath() + "/no-file.avsc] not found");
    }

    @Test
    public void shouldDeserializeValueWithLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_SCHEMA_FILE, "value.avsc");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }
}
