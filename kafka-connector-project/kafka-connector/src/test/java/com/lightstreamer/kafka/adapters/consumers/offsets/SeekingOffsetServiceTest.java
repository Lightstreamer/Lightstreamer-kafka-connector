
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

import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Tests for {@link SeekingOffsetService}.
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li><b>Seeking behavior</b>
 *       <ul>
 *         <li>{@code shouldSeekNewPartitionsToBeginning} — first assignment seeks to 0
 *         <li>{@code shouldNotSeekOnStableRebalance} — same partitions re-assigned, no re-seek
 *         <li>{@code shouldNotReseekAlreadyKnownPartitions} — known + new: only new seeked
 *       </ul>
 *   <li><b>End offsets</b>
 *       <ul>
 *         <li>{@code shouldCaptureEndOffsetsOnAssignment} — captures end offsets for assignment
 *         <li>{@code shouldHandleEmptyAssignment} — empty set yields empty end offsets
 *         <li>{@code shouldUpdateEndOffsetsOnSubsequentAssignment} — replaced on each assignment
 *         <li>{@code shouldReturnNullEndOffsetsBeforeFirstAssignment} — initial state is null
 *       </ul>
 *   <li><b>Delegation</b>
 *       <ul>
 *         <li>{@code shouldDelegateOnPartitionsAssigned} — forwards to delegate
 *         <li>{@code shouldDelegatePartitionLifecycleEvent} — parameterized: revoked/lost
 *         <li>{@code shouldDelegateMaybeCommit}
 *         <li>{@code shouldDelegateUpdateOffsets}
 *         <li>{@code shouldDelegateOnAsyncFailure}
 *         <li>{@code shouldDelegateOnConsumerShutdown}
 *         <li>{@code shouldDelegateGetFirstFailure}
 *         <li>{@code shouldDelegateOffsetsSnapshot}
 *       </ul>
 *   <li><b>Factory</b>
 *       <ul>
 *         <li>{@code shouldBeCreatableViaFactoryMethod} — wiring via {@code
 *             OffsetService.seekingCommit()}
 *       </ul>
 * </ul>
 */
public class SeekingOffsetServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(SeekingOffsetServiceTest.class);
    private static final String TOPIC = "topic";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
    private static final TopicPartition TP2 = new TopicPartition(TOPIC, 2);

    private MockConsumer mockConsumer;
    private SpyOffsetService delegate;
    private SeekingOffsetService service;

    @BeforeEach
    public void setUp() {
        mockConsumer = new MockConsumer(StrategyType.EARLIEST.toString());
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(TP0, 0L);
        beginningOffsets.put(TP1, 0L);
        beginningOffsets.put(TP2, 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);

        HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 100L);
        endOffsets.put(TP1, 200L);
        endOffsets.put(TP2, 300L);
        mockConsumer.updateEndOffsets(endOffsets);

        // Assign all partitions so that seek/position calls are valid
        mockConsumer.assign(List.of(TP0, TP1, TP2));

        delegate = new SpyOffsetService();
        service = new SeekingOffsetService(delegate, mockConsumer, logger);
    }

    @Test
    public void shouldSeekNewPartitionsToBeginning() {
        service.onPartitionsAssigned(Set.of(TP0, TP1));

        // Verify positions are at beginning (0) for both partitions
        assertThat(mockConsumer.position(TP0)).isEqualTo(0L);
        assertThat(mockConsumer.position(TP1)).isEqualTo(0L);
    }

    @Test
    public void shouldNotSeekOnStableRebalance() {
        // First assignment: TP0, TP1
        service.onPartitionsAssigned(Set.of(TP0, TP1));

        // Simulate consumer advancing positions
        mockConsumer.seek(TP0, 30L);
        mockConsumer.seek(TP1, 40L);

        // Stable rebalance: same partitions re-assigned
        service.onPartitionsAssigned(Set.of(TP0, TP1));

        // Positions should remain unchanged — no seeking
        assertThat(mockConsumer.position(TP0)).isEqualTo(30L);
        assertThat(mockConsumer.position(TP1)).isEqualTo(40L);
    }

    @Test
    public void shouldNotReseekAlreadyKnownPartitions() {
        // First assignment: TP0, TP1
        service.onPartitionsAssigned(Set.of(TP0, TP1));

        // Simulate consumer advancing position on TP0
        mockConsumer.seek(TP0, 50L);
        assertThat(mockConsumer.position(TP0)).isEqualTo(50L);

        // Second assignment: TP0 (re-assigned) + TP2 (new)
        service.onPartitionsAssigned(Set.of(TP0, TP2));

        // TP0 should NOT be re-seeked — still at 50
        assertThat(mockConsumer.position(TP0)).isEqualTo(50L);
        // TP2 is new and should be seeked to beginning
        assertThat(mockConsumer.position(TP2)).isEqualTo(0L);
    }

    @Test
    public void shouldCaptureEndOffsetsOnAssignment() {
        service.onPartitionsAssigned(Set.of(TP0, TP1));

        Map<TopicPartition, Long> catchUpEndOffsets = service.getCatchUpEndOffsets();
        assertThat(catchUpEndOffsets).containsExactly(TP0, 100L, TP1, 200L);
    }

    @Test
    public void shouldHandleEmptyAssignment() {
        // First assignment to populate knownPartitions
        service.onPartitionsAssigned(Set.of(TP0, TP1));
        mockConsumer.seek(TP0, 25L);

        // Empty assignment (all partitions revoked)
        service.onPartitionsAssigned(Set.of());

        // End offsets should be empty
        assertThat(service.getCatchUpEndOffsets()).isEmpty();
    }

    @Test
    public void shouldUpdateEndOffsetsOnSubsequentAssignment() {
        service.onPartitionsAssigned(Set.of(TP0));
        assertThat(service.getCatchUpEndOffsets()).containsExactly(TP0, 100L);

        // Second assignment with different partitions updates the end offsets
        service.onPartitionsAssigned(Set.of(TP1, TP2));
        assertThat(service.getCatchUpEndOffsets()).containsExactly(TP1, 200L, TP2, 300L);
    }

    @Test
    public void shouldReturnNullEndOffsetsBeforeFirstAssignment() {
        assertThat(service.getCatchUpEndOffsets()).isNull();
    }

    @Test
    public void shouldDelegateOnPartitionsAssigned() {
        Set<TopicPartition> partitions = Set.of(TP0, TP1);
        service.onPartitionsAssigned(partitions);

        assertThat(delegate.lastAssigned).containsExactlyElementsIn(partitions);
    }

    @ParameterizedTest
    @MethodSource("partitionLifecycleEvents")
    public void shouldDelegatePartitionLifecycleEvent(
            String event, Collection<TopicPartition> partitions) {
        switch (event) {
            case "revoked" -> service.onPartitionsRevoked(partitions);
            case "lost" -> service.onPartitionsLost(partitions);
            default -> throw new IllegalArgumentException(event);
        }

        Collection<TopicPartition> delegated =
                switch (event) {
                    case "revoked" -> delegate.lastRevoked;
                    case "lost" -> delegate.lastLost;
                    default -> throw new IllegalArgumentException(event);
                };
        assertThat(delegated).containsExactlyElementsIn(partitions);
    }

    static Stream<Arguments> partitionLifecycleEvents() {
        return Stream.of(
                Arguments.of("revoked", List.of(TP0)), Arguments.of("lost", List.of(TP0, TP1)));
    }

    @Test
    public void shouldDelegateMaybeCommit() {
        service.maybeCommit();

        assertThat(delegate.maybeCommitCalled).isTrue();
    }

    @Test
    public void shouldDelegateUpdateOffsets() {
        KafkaRecord<?, ?> record = KafkaRecord.from(TOPIC, 0, 42, 0L, "key", "value", null);
        service.updateOffsets(record);

        assertThat(delegate.lastUpdatedRecord).isSameInstanceAs(record);
    }

    @Test
    public void shouldDelegateOnAsyncFailure() {
        RuntimeException failure = new RuntimeException("test");
        service.onAsyncFailure(failure);

        assertThat(delegate.lastAsyncFailure).isSameInstanceAs(failure);
    }

    @Test
    public void shouldDelegateOnConsumerShutdown() {
        service.onConsumerShutdown();

        assertThat(delegate.shutdownCalled).isTrue();
    }

    @Test
    public void shouldDelegateGetFirstFailure() {
        RuntimeException failure = new RuntimeException("delegate failure");
        delegate.firstFailure = failure;

        assertThat(service.getFirstFailure()).isSameInstanceAs(failure);
    }

    @Test
    public void shouldDelegateOffsetsSnapshot() {
        Map<TopicPartition, OffsetAndMetadata> snapshot = Map.of(TP0, new OffsetAndMetadata(10L));
        delegate.snapshot = snapshot;

        assertThat(service.offsetsSnapshot()).isEqualTo(snapshot);
    }

    @Test
    public void shouldBeCreatableViaFactoryMethod() {
        OffsetService seekingService = OffsetService.seekingCommit(mockConsumer, logger);
        assertThat(seekingService).isInstanceOf(SeekingOffsetService.class);
    }

    /** Hand-written spy that records all delegate calls for verification. */
    private static class SpyOffsetService implements OffsetService {

        Collection<TopicPartition> lastAssigned;
        Collection<TopicPartition> lastRevoked;
        Collection<TopicPartition> lastLost;
        boolean maybeCommitCalled;
        KafkaRecord<?, ?> lastUpdatedRecord;
        Throwable lastAsyncFailure;
        boolean shutdownCalled;
        Throwable firstFailure;
        Map<TopicPartition, OffsetAndMetadata> snapshot = Map.of();

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            lastAssigned = partitions;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            lastRevoked = partitions;
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            lastLost = partitions;
        }

        @Override
        public void maybeCommit() {
            maybeCommitCalled = true;
        }

        @Override
        public void updateOffsets(KafkaRecord<?, ?> record) {
            lastUpdatedRecord = record;
        }

        @Override
        public void onAsyncFailure(Throwable th) {
            lastAsyncFailure = th;
        }

        @Override
        public void onConsumerShutdown() {
            shutdownCalled = true;
        }

        @Override
        public Throwable getFirstFailure() {
            return firstFailure;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
            return snapshot;
        }
    }
}
