
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

package com.lightstreamer.kafka.adapters.mapping.selectors.protobuf;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.PROTOBUF;

import com.google.protobuf.DynamicMessage;
import com.lightstreamer.example.Person;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageDeserializers.DynamicMessageLocalDeserializer;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class DynamicMessageDeserializerTest {

    private static final String SCHEMA_FOLDER = "src/test/resources";
    private static final String TEST_SCHEMA_FILE = "person.proto.desc";

    @Test
    public void shouldDeserializeWithLocalSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person"));

        Person person = Person.newBuilder().setName("John Doe").setAge(30).build();

        try (Deserializer<DynamicMessage> deserializer =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            DynamicMessage message = deserializer.deserialize("topic", person.toByteArray());
            assertThat(message).isNotNull();
            assertThat(message.getField(message.getDescriptorForType().findFieldByName("name")))
                    .isEqualTo("John Doe");
            assertThat(message.getField(message.getDescriptorForType().findFieldByName("age")))
                    .isEqualTo(30);
        }
    }

    @Test
    public void shouldDeserializeNullWithLocalSchema() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person"));

        try (Deserializer<DynamicMessage> deserializer =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            DynamicMessage message = deserializer.deserialize("topic", null);
            assertThat(message).isNull();
        }
    }

    @Test
    public void shouldGeKeyDeserializerWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<DynamicMessage> deserializer =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }
    }

    @Test
    public void shouldGetValueDeserializerWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<DynamicMessage> deserializer =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deserializer.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyAndValueDeserializerWithSchemaRegistry() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }
    }

    @Test
    public void shouldGetValueDeserializerWithLocalSchema() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }
    }

    @Test
    public void shouldGetKeyDeserializeWithSchemaRegistryValueDeserializerWithLocalSchema()
            throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }
        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchemaValueWithSchemaRegistry() throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(KafkaProtobufDeserializer.class);
        }
    }

    @Test
    public void shouldDeserializationWithLocalSchemaTakesPrecedenceOverSchemaRegistry()
            throws IOException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        SCHEMA_FOLDER,
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person",
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8080",
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                TEST_SCHEMA_FILE,
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "Person"));

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.KeyDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }

        try (Deserializer<DynamicMessage> deser =
                DynamicMessageDeserializers.ValueDeserializer(config)) {
            assertThat(deser.getClass()).isEqualTo(DynamicMessageLocalDeserializer.class);
        }
    }
}
