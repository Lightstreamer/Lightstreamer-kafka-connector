
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.records;

import static com.google.common.truth.Truth.assertThat;

import static org.apache.kafka.common.serialization.Serdes.String;

import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeaders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

public class KafkaRecordTest {

    private KafkaRecord.DeserializerPair<String, String> deserializerPair =
            new KafkaRecord.DeserializerPair<>(String().deserializer(), String().deserializer());

    static Stream<Arguments> consumerRecords() {
        return Stream.of(
                Arguments.of(
                        "test-topic",
                        "key".getBytes(),
                        "value".getBytes(),
                        "key",
                        "value",
                        0,
                        150L,
                        100L,
                        new RecordHeaders().add("key", "value".getBytes())),
                Arguments.of(
                        "another-topic",
                        null,
                        "another-value".getBytes(),
                        null,
                        "another-value",
                        2,
                        200L,
                        250L,
                        new RecordHeaders().add("key", "value".getBytes())),
                Arguments.of(
                        "third-topic",
                        "third-key".getBytes(),
                        null,
                        "third-key",
                        null,
                        1,
                        300L,
                        350L,
                        new RecordHeaders().add("key", "value ".getBytes())),
                Arguments.of(
                        "fourth-topic",
                        new byte[0],
                        new byte[0],
                        "",
                        "",
                        3,
                        400L,
                        450L,
                        new RecordHeaders().add("key", "value".getBytes())));
    }

    @ParameterizedTest
    @MethodSource("consumerRecords")
    public void shouldCreateDeferredRecord(
            String expectedTopic,
            byte[] rawKey,
            byte[] rawValue,
            String expectedKey,
            String expectedValue,
            int expectedPartition,
            long expectedTimestamp,
            long expectedOffset,
            RecordHeaders headers) {
        int keySize = rawKey == null ? ConsumerRecord.NULL_SIZE : rawKey.length;
        int valueSize = rawValue == null ? ConsumerRecord.NULL_SIZE : rawValue.length;

        ConsumerRecord<byte[], byte[]> consumerRecord =
                new ConsumerRecord<>(
                        expectedTopic,
                        expectedPartition,
                        expectedOffset,
                        expectedTimestamp,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        keySize,
                        valueSize,
                        rawKey,
                        rawValue,
                        headers,
                        Optional.empty());

        NotifyingRecordBatch<String, String> batch = new NotifyingRecordBatch<>(1);
        KafkaRecord<String, String> kafkaRecord =
                KafkaRecord.fromDeferred(consumerRecord, deserializerPair, batch);

        // Verify metadata
        assertThat(kafkaRecord.topic()).isEqualTo(expectedTopic);
        assertThat(kafkaRecord.partition()).isEqualTo(expectedPartition);
        assertThat(kafkaRecord.offset()).isEqualTo(expectedOffset);
        assertThat(kafkaRecord.timestamp()).isEqualTo(expectedTimestamp);

        // Verify key and value with caching
        String key = kafkaRecord.key();
        assertThat(key).isEqualTo(expectedKey);
        assertThat(kafkaRecord.key()).isSameInstanceAs(key);

        String value = kafkaRecord.value();
        assertThat(value).isEqualTo(expectedValue);
        assertThat(kafkaRecord.value()).isSameInstanceAs(value);

        // Verify headers
        KafkaHeaders kafkaHeaders = kafkaRecord.headers();
        assertThat(kafkaHeaders.has("key")).isTrue();

        // Verify payload null status
        assertThat(kafkaRecord.isPayloadNull()).isEqualTo(expectedValue == null);

        // Verify batch association
        assertThat(kafkaRecord.getBatch()).isSameInstanceAs(batch);
    }

    @ParameterizedTest
    @MethodSource("consumerRecords")
    public void shouldCreateEagerRecord(
            String expectedTopic,
            byte[] rawKey,
            byte[] rawValue,
            String expectedKey,
            String expectedValue,
            int expectedPartition,
            long expectedTimestamp,
            long expectedOffset,
            RecordHeaders headers) {
        int keySize = rawKey == null ? ConsumerRecord.NULL_SIZE : rawKey.length;
        int valueSize = rawValue == null ? ConsumerRecord.NULL_SIZE : rawValue.length;

        ConsumerRecord<byte[], byte[]> consumerRecord =
                new ConsumerRecord<>(
                        expectedTopic,
                        expectedPartition,
                        expectedOffset,
                        expectedTimestamp,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        keySize,
                        valueSize,
                        rawKey,
                        rawValue,
                        headers,
                        Optional.empty());

        NotifyingRecordBatch<String, String> batch = new NotifyingRecordBatch<>(1);
        KafkaRecord<String, String> kafkaRecord =
                KafkaRecord.fromEager(consumerRecord, deserializerPair, batch);

        // Verify metadata
        assertThat(kafkaRecord.topic()).isEqualTo(expectedTopic);
        assertThat(kafkaRecord.partition()).isEqualTo(expectedPartition);
        assertThat(kafkaRecord.offset()).isEqualTo(expectedOffset);
        assertThat(kafkaRecord.timestamp()).isEqualTo(expectedTimestamp);

        // Verify key and value
        assertThat(kafkaRecord.key()).isEqualTo(expectedKey);
        assertThat(kafkaRecord.value()).isEqualTo(expectedValue);

        // Verify headers
        KafkaHeaders kafkaHeaders = kafkaRecord.headers();
        assertThat(kafkaHeaders.has("key")).isTrue();

        // Verify payload null status
        assertThat(kafkaRecord.isPayloadNull()).isEqualTo(expectedValue == null);

        // Verify batch association
        assertThat(kafkaRecord.getBatch()).isSameInstanceAs(batch);
    }

    static Stream<Arguments> sinkRecords() {
        return Stream.of(
                Arguments.of("test-topic", "test-key", "test-value", 5, 100L, 100L, false),
                Arguments.of("another-topic", null, "another-value", 2, 200L, 200L, false),
                Arguments.of("third-topic", "third-key", null, 1, 300L, 300L, true),
                Arguments.of("fourth-topic", null, null, 3, 400L, 400L, true));
    }

    @ParameterizedTest
    @MethodSource("sinkRecords")
    public void shouldCreateSinkRecord(
            String expectedTopic,
            Object expectedKey,
            Object expectedValue,
            int expectedPartition,
            long expectedOffset,
            long expectedTimestamp,
            boolean expectedPayloadNull) {
        SinkRecord sinkRecord =
                new SinkRecord(
                        expectedTopic,
                        expectedPartition,
                        null,
                        expectedKey,
                        null,
                        expectedValue,
                        expectedOffset,
                        expectedTimestamp,
                        TimestampType.NO_TIMESTAMP_TYPE);

        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        // Verify metadata
        assertThat(kafkaRecord.topic()).isEqualTo(expectedTopic);
        assertThat(kafkaRecord.partition()).isEqualTo(expectedPartition);
        assertThat(kafkaRecord.offset()).isEqualTo(expectedOffset);
        assertThat(kafkaRecord.timestamp()).isEqualTo(expectedTimestamp);

        // Verify key and value
        assertThat(kafkaRecord.key()).isEqualTo(expectedKey);
        assertThat(kafkaRecord.value()).isEqualTo(expectedValue);

        // Verify headers
        KafkaHeaders kafkaHeaders = kafkaRecord.headers();
        assertThat(kafkaHeaders).isNotNull();

        // Verify payload null status
        assertThat(kafkaRecord.isPayloadNull()).isEqualTo(expectedPayloadNull);

        // Verify batch association
        assertThat(kafkaRecord.getBatch()).isNull();
    }
}
