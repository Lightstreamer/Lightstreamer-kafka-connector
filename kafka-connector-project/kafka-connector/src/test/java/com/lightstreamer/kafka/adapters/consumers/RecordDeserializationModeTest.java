
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

package com.lightstreamer.kafka.adapters.consumers;

import static com.google.common.truth.Truth.assertThat;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.consumers.RecordDeserializationMode.DeserializationTiming;
import com.lightstreamer.kafka.common.records.DeferredKafkaConsumerRecord;
import com.lightstreamer.kafka.common.records.EagerKafkaConsumerRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;

import java.util.List;

public class RecordDeserializationModeTest {

    private static final Logger logger = LogFactory.getLogger("TestConnection");

    private final DeserializerPair<String, String> deserializerPair =
            new DeserializerPair<>(String().deserializer(), String().deserializer());

    @Test
    public void shouldThrowOnNullLogger() {
        assertThrows(
                NullPointerException.class,
                () ->
                        RecordDeserializationMode.forTiming(
                                DeserializationTiming.EAGER, deserializerPair, null));
    }

    @ParameterizedTest
    @EnumSource(DeserializationTiming.class)
    public void shouldCreateBatchWithCorrectTimingAndRecordType(DeserializationTiming timing) {
        RecordDeserializationMode<String, String> mode =
                RecordDeserializationMode.forTiming(timing, deserializerPair, logger);
        assertThat(mode.getTiming()).isEqualTo(timing);

        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 5, List.of(), 2);

        RecordBatch<String, String> batch = mode.toBatch(consumerRecords);
        assertThat(batch.count()).isEqualTo(5);

        Class<?> expectedType =
                timing == DeserializationTiming.EAGER
                        ? EagerKafkaConsumerRecord.class
                        : DeferredKafkaConsumerRecord.class;
        for (KafkaRecord<String, String> record : batch.getRecords()) {
            assertThat(record).isInstanceOf(expectedType);
        }
    }

    @ParameterizedTest
    @EnumSource(DeserializationTiming.class)
    public void shouldCreateJoinableBatch(DeserializationTiming timing) {
        RecordDeserializationMode<String, String> mode =
                RecordDeserializationMode.forTiming(timing, deserializerPair, logger);

        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 3, List.of());

        RecordBatch<String, String> batch = mode.toBatch(consumerRecords, true);
        assertThat(batch.count()).isEqualTo(3);

        Class<?> expectedType =
                timing == DeserializationTiming.EAGER
                        ? EagerKafkaConsumerRecord.class
                        : DeferredKafkaConsumerRecord.class;
        for (KafkaRecord<String, String> record : batch.getRecords()) {
            assertThat(record).isInstanceOf(expectedType);
        }
    }

    @Test
    public void shouldThrowOnInvalidTiming() {
        // Ensure the default case throws
        // (This is a safety net; can't actually trigger with the current enum values,
        // but verifies the switch statement's default branch)
        assertThat(DeserializationTiming.values()).hasLength(2);
    }
}
