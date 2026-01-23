
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

import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeaders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class KafkaRecordTest {

    private Deserializer<String> keyDeserializer = Serdes.String().deserializer();
    private Deserializer<String> valueDeserializer = Serdes.String().deserializer();

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

        KafkaRecord<String, String> kafkaRecord =
                KafkaRecord.fromDeferred(consumerRecord, keyDeserializer, valueDeserializer);

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

        KafkaRecord<String, String> kafkaRecord =
                KafkaRecord.fromEager(consumerRecord, keyDeserializer, valueDeserializer);

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
    }

    @Test
    public void shouldCreateListFromDeferred() {
        int partitions = 2;
        int recordsPerPartitions = 10;
        ConsumerRecords<byte[], byte[]> records =
                makeRecords("topic", partitions, recordsPerPartitions);
        List<KafkaRecord<String, String>> deferred =
                KafkaRecord.listFromDeferred(records, keyDeserializer, valueDeserializer);

        // Verify total size
        assertThat(deferred).hasSize(partitions * recordsPerPartitions);

        // Verify records per partition
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            assertThat(deferred.stream().filter(k -> k.partition() == partition).count())
                    .isEqualTo(recordsPerPartitions);
        }

        // Verify record content and order are preserved
        int index = 0;
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < recordsPerPartitions; o++) {
                KafkaRecord<String, String> record = deferred.get(index++);
                assertThat(record.partition()).isEqualTo(p);
                assertThat(record.offset()).isEqualTo(o);
                assertThat(record.topic()).isEqualTo("topic");
                assertThat(record.key()).isEqualTo("aKey-" + o);
                assertThat(record.value()).isEqualTo("aValue-" + o);
            }
        }

        // Verify deferred deserialization (key and value are cached after first access)
        KafkaRecord<String, String> firstRecord = deferred.get(0);
        String key = firstRecord.key();
        assertThat(firstRecord.key()).isSameInstanceAs(key);
        String value = firstRecord.value();
        assertThat(firstRecord.value()).isSameInstanceAs(value);
    }

    @Test
    public void shouldCreateListFromEager() {
        int partitions = 2;
        int recordsPerPartitions = 10;
        ConsumerRecords<byte[], byte[]> records =
                makeRecords("topic", partitions, recordsPerPartitions);
        List<KafkaRecord<String, String>> eager =
                KafkaRecord.listFromEager(records, keyDeserializer, valueDeserializer);

        // Verify total size
        assertThat(eager).hasSize(partitions * recordsPerPartitions);

        // Verify records per partition
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            assertThat(eager.stream().filter(k -> k.partition() == partition).count())
                    .isEqualTo(recordsPerPartitions);
        }

        // Verify record content and order are preserved
        int index = 0;
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < recordsPerPartitions; o++) {
                KafkaRecord<String, String> record = eager.get(index++);
                assertThat(record.partition()).isEqualTo(p);
                assertThat(record.offset()).isEqualTo(o);
                assertThat(record.topic()).isEqualTo("topic");
                assertThat(record.key()).isEqualTo("aKey-" + o);
                assertThat(record.value()).isEqualTo("aValue-" + o);
            }
        }
    }

    static ConsumerRecords<byte[], byte[]> makeRecords(
            String topic, int partitions, int recordsPerPartitions) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < recordsPerPartitions; o++) {
                TopicPartition tp = new TopicPartition(topic, p);
                recordsMap
                        .computeIfAbsent(tp, t -> new ArrayList<>())
                        .add(
                                new ConsumerRecord<>(
                                        "topic",
                                        p,
                                        o,
                                        ("aKey-" + o).getBytes(),
                                        ("aValue-" + o).getBytes()));
            }
        }

        return new ConsumerRecords<>(recordsMap);
    }
}
