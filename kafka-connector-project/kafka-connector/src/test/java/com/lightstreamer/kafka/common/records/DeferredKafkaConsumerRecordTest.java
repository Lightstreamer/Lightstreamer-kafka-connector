
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

public class DeferredKafkaConsumerRecordTest {

    static Stream<Arguments> keyDeserializationArgs() {
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
    @MethodSource("keyDeserializationArgs")
    public void shouldCreateRecord(
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

        Serdes.String().serializer(); // just to avoid warnings

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

        Deserializer<String> keyDeserializer = Serdes.String().deserializer();
        Deserializer<String> valueDeserializer = Serdes.String().deserializer();
        KafkaRecord<String, String> kafkaRecord =
                KafkaRecord.fromDeferred(consumerRecord, keyDeserializer, valueDeserializer);
        assertThat(kafkaRecord.topic()).isEqualTo(expectedTopic);

        String key = kafkaRecord.key();
        assertThat(key).isEqualTo(expectedKey);
        // Check caching
        assertThat(kafkaRecord.key()).isSameInstanceAs(key);

        String value = kafkaRecord.value();
        assertThat(value).isEqualTo(expectedValue);
        // Check caching
        assertThat(kafkaRecord.value()).isSameInstanceAs(value);

        assertThat(kafkaRecord.timestamp()).isEqualTo(expectedTimestamp);
        assertThat(kafkaRecord.partition()).isEqualTo(expectedPartition);
        assertThat(kafkaRecord.offset()).isEqualTo(expectedOffset);

        KafkaHeaders kafkaHeaders = kafkaRecord.headers();
        assertThat(kafkaHeaders.has("key")).isTrue();

        if (expectedValue == null || expectedValue.isEmpty()) {
            assertThat(kafkaRecord.isPayloadNull()).isTrue();
        } else {
            assertThat(kafkaRecord.isPayloadNull()).isFalse();
        }
    }

    static Stream<byte[]> nullPayload() {
        return Stream.of(null, new byte[0]);
    }
}
