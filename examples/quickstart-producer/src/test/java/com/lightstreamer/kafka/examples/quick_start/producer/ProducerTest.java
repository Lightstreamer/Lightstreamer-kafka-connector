
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.examples.quick_start.producer.json.JsonStock;
import com.lightstreamer.kafka.examples.quick_start.producer.protobuf.ProtobufStock;
import com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerTest {

    private KafkaProducer<Integer, Object> mockProducer;

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
                        DESERIALIZER,                                                    TYPE
                        io.confluent.kafka.serializers.KafkaJsonSerializer,              JSON
                        io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer,   JSON_SCHEMA
                        io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer, PROTOBUF
                        """)
    public void shouldConfigure(String deserializerClass, String serializerType) throws Exception {
        Properties properties = new Properties();
        properties.put("value.serializer", deserializerClass);
        producer.configure(properties);
        assertThat(producer.getSerializerType().toString()).isEqualTo(serializerType);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"com.example.UnknownSerializer", " ", "\n", "\t", "<NULL>"})
    public void shouldNotConfigure(String deserializerClass) throws Exception {
        Producer producer = new Producer("localhost:9092", "test-topic");
        Properties properties = new Properties();
        if (!"<NULL>".equals(deserializerClass)) {
            properties.put("value.serializer", deserializerClass);
        }
        assertThrows(RuntimeException.class, () -> producer.configure(properties));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        DESERIALIZER,                                                    TYPE,         MESSAGE_CLASS
                        io.confluent.kafka.serializers.KafkaJsonSerializer,              JSON,         com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer,   JSON_SCHEMA,  com.lightstreamer.kafka.examples.quick_start.producer.json.Stock
                        io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer, PROTOBUF,     com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock
                        """)
    public void shouldConvertToRightRecordClass(
            String deserializerClass, String serializerType, String messageClass) {
        Properties properties = new Properties();
        properties.put("value.serializer", deserializerClass);
        producer.configure(properties);

        // Create test stock event
        Map<String, String> stockEvent = createStockEvent();

        // Invoke toRecord method
        Object result = producer.toRecord(stockEvent);

        // Verify the resulting class
        assertThat(result.getClass().getName()).isEqualTo(messageClass);
    }

    @Test
    public void shouldCreateJsonStockFromEvent() {
        // Create test stock event
        Map<String, String> stockEvent = createStockEvent();

        JsonStock stock = JsonStock.fromEvent(stockEvent);
        assertNotNull(stock);
        assertThat(stock.name()).isEqualTo("AAPL");
        assertThat(stock.time()).isEqualTo("12:34:56");
        assertThat(stock.timestamp()).isEqualTo("2023-10-01T12:34:56Z");
        assertThat(stock.ask()).isEqualTo("150.50");
        assertThat(stock.bid()).isEqualTo("149.75");
        assertThat(stock.ask_quantity()).isEqualTo("50");
        assertThat(stock.bid_quantity()).isEqualTo("100");
        assertThat(stock.pct_change()).isEqualTo("0.5");
        assertThat(stock.min()).isEqualTo("149.00");
        assertThat(stock.max()).isEqualTo("151.00");
        assertThat(stock.ref_price()).isEqualTo("150.00");
        assertThat(stock.open_price()).isEqualTo("149.50");
        assertThat(stock.item_status()).isEqualTo("active");
    }

    @Test
    public void shouldCreateProtobufStockFromEvent() {
        // Create test stock event
        Map<String, String> stockEvent = createStockEvent();

        Stock stock = ProtobufStock.fromEvent(stockEvent);
        assertNotNull(stock);
        assertThat(stock.getName()).isEqualTo("AAPL");
        assertThat(stock.getTime()).isEqualTo("12:34:56");
        assertThat(stock.getTimestamp()).isEqualTo("2023-10-01T12:34:56Z");
        assertThat(stock.getAsk()).isEqualTo("150.50");
        assertThat(stock.getBid()).isEqualTo("149.75");
        assertThat(stock.getAskQuantity()).isEqualTo("50");
        assertThat(stock.getBidQuantity()).isEqualTo("100");
        assertThat(stock.getPctChange()).isEqualTo("0.5");
        assertThat(stock.getMin()).isEqualTo("149.00");
        assertThat(stock.getMax()).isEqualTo("151.00");
        assertThat(stock.getRefPrice()).isEqualTo("150.00");
        assertThat(stock.getOpenPrice()).isEqualTo("149.50");
        assertThat(stock.getItemStatus()).isEqualTo("active");
    }

    // @Test
    // public void testOnEvent() throws Exception {
    //     // Set up producer
    //     setField(producer, "topic", "test-topic");
    //     setField(producer, "serializerType", Producer.SerializerType.JSON);
    //     setField(producer, "producer", mockProducer);

    //     // Create test stock event
    //     Map<String, String> stockEvent = new HashMap<>();
    //     stockEvent.put("name", "AAPL");
    //     stockEvent.put("last", "150.5");

    //     // Call onEvent
    //     producer.onEvent(1, stockEvent);

    //     // Verify that send was called with correct ProducerRecord
    //     ArgumentCaptor<ProducerRecord<Integer, Object>> recordCaptor =
    //             ArgumentCaptor.forClass(ProducerRecord.class);
    //     verify(mockProducer).send(recordCaptor.capture(), any());

    //     ProducerRecord<Integer, Object> capturedRecord = recordCaptor.getValue();
    //     assertEquals("test-topic", capturedRecord.topic());
    //     assertEquals(1, capturedRecord.key());
    //     assertTrue(capturedRecord.value() instanceof JsonStock);
    // }

    private Map<String, String> createStockEvent() {
        Map<String, String> stockEvent = new HashMap<>();
        stockEvent.put("name", "AAPL");
        stockEvent.put("time", "12:34:56");
        stockEvent.put("timestamp", "2023-10-01T12:34:56Z");
        stockEvent.put("last_price", "150.5");
        stockEvent.put("ask", "150.50");
        stockEvent.put("bid", "149.75");
        stockEvent.put("ask_quantity", "50");
        stockEvent.put("bid_quantity", "100");
        stockEvent.put("pct_change", "0.5");
        stockEvent.put("min", "149.00");
        stockEvent.put("max", "151.00");
        stockEvent.put("ref_price", "150.00");
        stockEvent.put("open_price", "149.50");
        stockEvent.put("item_status", "active");
        return stockEvent;
    }
}
