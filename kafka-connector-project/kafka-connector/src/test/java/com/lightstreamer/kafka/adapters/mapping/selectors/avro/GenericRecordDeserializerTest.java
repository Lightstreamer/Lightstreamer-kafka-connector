
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
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class GenericRecordDeserializerTest {

    private static final String SCHEMA_FOLDER = "src/test/resources";
    private static final String TEST_SCHEMA_FILE = "test_schema.avsc";

    private static byte[] serializeRecord(GenericRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        byte[] bytes = outputStream.toByteArray();
        return bytes;
    }

    @Test
    public void shouldDeserializeWithLocalSchema() throws IOException {
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
    public void shouldGetKeyDeserializerWithSchemaRegsitry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
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
    public void shouldGetValueDeserializerWithSchemaRegsitry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
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
    public void shouldGetKeyAndVaueDeserializeWithSchemaRegisstry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
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
    public void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
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
    public void shouldGetValueDeserializerWithLocalSchema() throws IOException {
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
    public void shouldGetKeyAndValueDeserializerWithLocalSchema() throws IOException {
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
    public void shouldGetKeyDeserializeWithSchemaRegistryValueDeserializerWithLocalSchema() {
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
    public void shouldDeserializeKeyWithLocalSchemaValueWithSchemaRegistry() {
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
    public void shouldDeserializazionWithLocalSchemaTakesPrecedenceOverSchemaRegistry()
            throws IOException {
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
}
