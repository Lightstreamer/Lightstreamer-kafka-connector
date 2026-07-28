
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

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializers.GenericRecordAzureSchemaRegistryDeserializer;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializers.GenericRecordLocalSchemaDeserializer;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializers.WrapperKafkaAvroDeserializer;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class GenericRecordDeserializerTest {

    private static final String SCHEMA_FOLDER = "src/test/resources";
    private static final String TEST_SCHEMA_FILE = "test_schema.avsc";

    private Path adapterDir;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
    }

    @AfterEach
    void after() throws IOException {
        FileUtils.deleteDirectory(adapterDir.toFile());
    }

    private static byte[] serializeRecord(GenericRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }

    @Test
    void shouldDeserializeWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        Schema.Parser parser = new Schema.Parser();
        Schema schema =
                parser.parse(
                        GenericRecordDeserializerTest.class
                                .getClassLoader()
                                .getResourceAsStream(TEST_SCHEMA_FILE));
        GenericRecord record = new GenericData.Record(schema);
        record.put("firstName", "John");
        record.put("lastName", "Doe");

        byte[] bytes = serializeRecord(record);

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            GenericRecord deserializedRecord = deserializer.deserialize("topic", bytes);
            assertThat(deserializedRecord.get("firstName")).isEqualTo(new Utf8("John"));
            assertThat(deserializedRecord.get("lastName")).isEqualTo(new Utf8("Doe"));
        }
    }

    @Test
    void shouldDeserializeNullWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            GenericRecord deserializedRecord = deserializer.deserialize("topic", null);
            assertThat(deserializedRecord).isNull();
            deserializedRecord = deserializer.deserialize("topic", new byte[0]);
            assertThat(deserializedRecord).isNull();
        }
    }

    @Test
    void shouldGetKeyDeserializerWithConfluentSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }
    }

    @Test
    void shouldGetKeyAndValueDeserializerWithConfluentSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
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
                                "http://localhost:8080"));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }
    }

    @Test
    void shouldGetValueDeserializerWithConfluentSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }
    }

    @Test
    void shouldGetKeyDeserializerWithAzureSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                "AZURE",
                                SchemaRegistryConfigs.AZURE_TENANT_ID,
                                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                SchemaRegistryConfigs.AZURE_CLIENT_ID,
                                "client-id",
                                SchemaRegistryConfigs.AZURE_CLIENT_SECRET,
                                "client-secret",
                                SchemaRegistryConfigs.URL,
                                "https://my-namespace.servicebus.windows.net"));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass())
                    .isEqualTo(GenericRecordAzureSchemaRegistryDeserializer.class);
        }
    }

    @Test
    void shouldGetValueDeserializerWithAzureSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                "AZURE",
                                SchemaRegistryConfigs.AZURE_TENANT_ID,
                                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                SchemaRegistryConfigs.AZURE_CLIENT_ID,
                                "client-id",
                                SchemaRegistryConfigs.AZURE_CLIENT_SECRET,
                                "client-secret",
                                SchemaRegistryConfigs.URL,
                                "https://my-namespace.servicebus.windows.net"));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass())
                    .isEqualTo(GenericRecordAzureSchemaRegistryDeserializer.class);
        }
    }

    @Test
    void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }

    @Test
    void shouldGetValueDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<GenericRecord> deserializer =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass())
                    .isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }

    @Test
    void shouldGetKeyAndValueDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }

    @Test
    public void
            shouldGetKeyDeserializerWithConfluentSchemaRegistryAndValueDeserializerWithLocalSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }

    @Test
    void shouldDeserializeKeyWithLocalSchemaAndValueWithConfluentSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(WrapperKafkaAvroDeserializer.class);
        }
    }

    @Test
    void shouldDeserializeKeyWithLocalSchemaAndValueWithAzureSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080",
                                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                "AZURE",
                                SchemaRegistryConfigs.AZURE_TENANT_ID,
                                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                SchemaRegistryConfigs.AZURE_CLIENT_ID,
                                "client-id",
                                SchemaRegistryConfigs.AZURE_CLIENT_SECRET,
                                "client-secret"));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass())
                    .isEqualTo(GenericRecordAzureSchemaRegistryDeserializer.class);
        }
    }

    @Test
    void shouldPreferLocalSchemaOverConfluentSchemaRegistry() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080",
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE));

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }

    @Test
    void shouldPreferLocalSchemaOverAzureSchemaRegistry() throws IOException {
        Map<String, String> configs =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        TEST_SCHEMA_FILE,
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        SchemaRegistryConfigs.URL,
                        "http://localhost:8080",
                        SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                        "AZURE",
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        TEST_SCHEMA_FILE);
        Map<String, String> configsWithAzureSettings = new HashMap<>(configs);
        configsWithAzureSettings.put(
                SchemaRegistryConfigs.AZURE_TENANT_ID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        configsWithAzureSettings.put(SchemaRegistryConfigs.AZURE_CLIENT_ID, "client-id");
        configsWithAzureSettings.put(SchemaRegistryConfigs.AZURE_CLIENT_SECRET, "client-secret");

        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(SCHEMA_FOLDER, configsWithAzureSettings);

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }

        try (Deserializer<GenericRecord> deser =
                GenericRecordDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(GenericRecordLocalSchemaDeserializer.class);
        }
    }
}
