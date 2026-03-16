
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.examples.quick_start.producer;

import static com.google.common.truth.Truth.assertThat;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.azure.core.credential.TokenCredential;
import com.lightstreamer.kafka.examples.quick_start.producer.test_utils.StockEvents;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;
import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

public class ProducerTest {

    private Producer producer;

    @BeforeEach
    public void setUp() {
        producer = new Producer();
        // In real usage, this would be provided via command-line argument, but for unit testing we
        // can set a default value.
        producer.topic = "test-topic";
    }

    @Test
    public void shouldBootstrapServersFromCommandLineTakesPrecedence() {
        producer.bootstrapServers = "cli-bootstrap:9092"; // Simulate command-line input
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producer.configure(properties);
        assertThat(producer.bootstrapServers).isEqualTo("cli-bootstrap:9092");
    }

    @Test
    public void shouldNotConfigureWithMissingBootstrapServers() {
        // Do not set bootstrap servers in either command-line argument or properties
        Properties properties = new Properties();
        assertThrows(IllegalArgumentException.class, () -> producer.configure(properties));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        DESERIALIZER,                                                                            TYPE,         MESSAGE_CLASS
                        io.confluent.kafka.serializers.KafkaJsonSerializer,                                      JSON,         com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer,                           JSON_SCHEMA,  com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer,                       JSON_SCHEMA,  com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer,                         PROTOBUF,     com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock
                        com.lightstreamer.kafka.examples.quick_start.producer.protobuf.CustomProtobufSerializer, PROTOBUF,     com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock
                        io.confluent.kafka.serializers.KafkaAvroSerializer,                                      AVRO,         com.lightstreamer.kafka.examples.quick_start.producer.avro.Stock
                        com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer,                       AVRO,         com.lightstreamer.kafka.examples.quick_start.producer.avro.Stock
                        <DEFAULT>,                                                                               JSON,         com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        """)
    public void shouldConfigureAndConvertToRightMessageClass(
            String deserializerClass, String serializerType, String messageClass) throws Exception {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Set the serializer class only if the input is not <DEFAULT>, which means to use the
        // default serializer class.
        if (!deserializerClass.equals("<DEFAULT>")) {
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, deserializerClass);
        }

        // The Azure serializers require credential properties; supply stubs for unit testing.
        if (Set.of(KafkaJsonSerializer.class.getName(), KafkaAvroSerializer.class.getName())
                .contains(deserializerClass)) {
            properties.put("tenant.id", "test-tenant");
            properties.put("client.id", "test-client");
            properties.put("client.secret", "test-secret");
        }

        Producer.SerializationFormat resolvedFormat = producer.configure(properties);
        assertThat(resolvedFormat.toString()).isEqualTo(serializerType);

        // Create test stock event
        Map<String, String> stockEvent = StockEvents.createEvent();

        // Invoke toValue method
        Object result = resolvedFormat.toValue(stockEvent);

        // Verify the resulting class
        assertThat(result.getClass().getName()).isEqualTo(messageClass);
    }

    static Stream<String> serializersRequiringAzureCredentials() {
        return Stream.of(KafkaJsonSerializer.class.getName(), KafkaAvroSerializer.class.getName());
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @MethodSource("serializersRequiringAzureCredentials")
    public void shouldRequireAzureCredentialProperties(String serializerClass) {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);

        // Do not set tenant.id
        RuntimeException re =
                assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'tenant.id'");

        // Set blank tenant.id
        properties.put("tenant.id", "");
        re = assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'tenant.id'");

        // Set tenant.id but do not set client.id
        properties.put("tenant.id", "test-tenant");
        re = assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'client.id'");

        // Set blank client.id
        properties.put("client.id", "");
        re = assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'client.id'");

        // Set client.id but do not set client.secret
        properties.put("client.id", "test-client");
        re = assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'client.secret'");

        // Set blank client.secret
        properties.put("client.secret", "");
        re = assertThrows(RuntimeException.class, () -> producer.configure(properties));
        assertThat(re)
                .hasMessageThat()
                .contains("Missing required Azure credential property: 'client.secret'");

        // Set all required properties - should not throw
        properties.put("client.secret", "test-secret");
        assertDoesNotThrow(() -> producer.configure(properties));

        assertThat(properties.get(Producer.AZURE_SCHEMA_REGISTRY_CREDENTIAL))
                .isInstanceOf(TokenCredential.class);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"com.example.UnknownSerializer", " ", "\n", "\t"})
    public void shouldNotConfigureWithInvalidDeserializer(String deserializerClass)
            throws Exception {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("value.serializer", deserializerClass);
        assertThrows(RuntimeException.class, () -> producer.configure(properties));
    }
}
