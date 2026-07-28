
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

class NoCommitOffsetServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(NoCommitOffsetServiceTest.class);
    private static final String TOPIC = "topic";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    private MockConsumer<?, ?> consumer;
    private NoCommitOffsetService offsetService;

    @BeforeEach
    void before() {
        consumer = new MockConsumer<>(StrategyType.LATEST.toString());
        consumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 0L));
        consumer.updateEndOffsets(Map.of(TP0, 100L, TP1, 200L));
        consumer.assign(List.of(TP0, TP1));
        offsetService = new NoCommitOffsetService(consumer, logger, RecordConsumeFrom.LATEST);
    }

    @Test
    void shouldReturnEmptyOffsetsSnapshot() {
        assertThat(offsetService.offsetsSnapshot()).isEmpty();
    }

    @Test
    void shouldReturnEmptyOffsetsSnapshotAfterUpdateOffsets() {
        offsetService.updateOffsets(KafkaRecord.from("topic", 0, 42, 0L, "key", "value", null));
        assertThat(offsetService.offsetsSnapshot()).isEmpty();
    }

    @Test
    void shouldReturnNullForGetFirstFailure() {
        assertThat(offsetService.getFirstFailure()).isNull();
    }

    @Test
    void shouldReturnNullForGetFirstFailureAfterOnAsyncFailure() {
        offsetService.onAsyncFailure(new RuntimeException("test failure"));
        assertThat(offsetService.getFirstFailure()).isNull();
    }

    @Test
    void shouldNotThrowOnMaybeCommit() {
        offsetService.maybeCommit();
        // No exception is thrown; the maybeCommit call is a no-op.
    }

    @Test
    void shouldNotThrowOnConsumerShutdown() {
        offsetService.onConsumerShutdown();
        // No exception is thrown; the onConsumerShutdown call is a no-op.
    }

    @ParameterizedTest
    @EnumSource(RecordConsumeFrom.class)
    void shouldSeekAssignedPartitionsPerConsumeFrom(RecordConsumeFrom from) {
        NoCommitOffsetService service = new NoCommitOffsetService(consumer, logger, from);

        service.onPartitionsAssigned(Set.of(TP0, TP1));

        // EARLIEST seeks to the beginning offsets (0); LATEST seeks to the end offsets set in
        // setUp.
        long expectedTp0 = from == RecordConsumeFrom.EARLIEST ? 0L : 100L;
        long expectedTp1 = from == RecordConsumeFrom.EARLIEST ? 0L : 200L;
        assertThat(consumer.position(TP0)).isEqualTo(expectedTp0);
        assertThat(consumer.position(TP1)).isEqualTo(expectedTp1);
    }

    @Test
    void shouldWarnAndDoNothingOnUnexpectedPartitionsRevoked() {
        offsetService.onPartitionsRevoked(List.of(TP0));
        // No exception is thrown; the callback logs a warning and performs no commit.
    }

    @Test
    void shouldWarnAndDoNothingOnUnexpectedPartitionsLost() {
        offsetService.onPartitionsLost(List.of(TP0));
        // No exception is thrown; the callback logs a warning and performs no cleanup.
    }

    @Test
    void shouldBeCreatableViaFactoryMethod() {
        OffsetService service = OffsetService.noCommit(consumer, logger, RecordConsumeFrom.LATEST);
        assertThat(service).isInstanceOf(NoCommitOffsetService.class);
        assertThat(service.offsetsSnapshot()).isEmpty();
        assertThat(service.getFirstFailure()).isNull();
    }
}
