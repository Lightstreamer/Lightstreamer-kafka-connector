
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.examples.quick_start.producer.test_utils.StockEvents;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Properties;

public class ProducerTest {

    private Producer producer;

    @BeforeEach
    public void setUp() {
        producer = new Producer("localhost:9092", "test-topic");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        DESERIALIZER,                                                                            TYPE,         MESSAGE_CLASS
                        io.confluent.kafka.serializers.KafkaJsonSerializer,                                      JSON,         com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer,                           JSON_SCHEMA,  com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer,                         PROTOBUF,     com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock
                        com.lightstreamer.kafka.examples.quick_start.producer.protobuf.CustomProtobufSerializer, PROTOBUF,     com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock
                        <DEFAULT>,                                                                               JSON,         com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        """)
    public void shouldConfigureAndConvertToRightMessageClass(
            String deserializerClass, String serializerType, String messageClass) throws Exception {
        Properties properties = new Properties();

        // Set the serializer class only if the input is not <DEFAULT>, which means to use the
        // default serializer class.
        if (!deserializerClass.equals("<DEFAULT>")) {
            properties.put("value.serializer", deserializerClass);
        }

        producer.configure(properties);
        assertThat(producer.getSerializerType().toString()).isEqualTo(serializerType);

        // Create test stock event
        Map<String, String> stockEvent = StockEvents.createEvent();

        // Invoke toRecord method
        Object result = producer.toRecord(stockEvent);

        // Verify the resulting class
        assertThat(result.getClass().getName()).isEqualTo(messageClass);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"com.example.UnknownSerializer", " ", "\n", "\t"})
    public void shouldNotConfigureWithInvalidDeserializer(String deserializerClass)
            throws Exception {
        Producer producer = new Producer("localhost:9092", "test-topic");
        Properties properties = new Properties();
        properties.put("value.serializer", deserializerClass);
        assertThrows(RuntimeException.class, () -> producer.configure(properties));
    }
}
