
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.Records.ConsumerRecord;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetStore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class OffsetServiceTest {

    private static final String TOPIC = "topic";

    private MockConsumer<String, String> mockConsumer;
    private OffsetService offsetService;
    private static final TopicPartition partition0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition partition1 = new TopicPartition(TOPIC, 1);

    @BeforeEach
    void beforeEach() {
        setUpFromEarliest();
        // setUpFromLatest();
    }

    void setUpFromEarliest() {
        setUp(true);
    }

    void setUpFromLatest() {
        setUp(false);
    }

    private void setUp(boolean earliest) {
        this.mockConsumer = new MockConsumer<>(earliest ? EARLIEST : LATEST);

        // A rebalance must be scheduled to later use the subscribe method
        this.mockConsumer.schedulePollTask(
                () -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 10L);
        offsets.put(partition1, 12L);
        if (earliest) {
            mockConsumer.updateBeginningOffsets(offsets);
        } else {
            mockConsumer.updateEndOffsets(offsets);
        }

        // Initialize the OffsetService
        this.offsetService =
                Offsets.OffsetService(
                        mockConsumer, true, LoggerFactory.getLogger(OffsetService.class));

        // Subscribe to topic specifying the OffsetService as ConsumerRebalancerListener
        this.mockConsumer.subscribe(Collections.singleton(TOPIC), offsetService);

        // A first poll is required to trigger partitions assignment.
        this.mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
    }

    static Stream<Arguments> initStoreArguments() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Map.of(partition0, 10L, partition1, 12L),
                        Map.of(
                                partition0,
                                new OffsetAndMetadata(10L),
                                partition1,
                                new OffsetAndMetadata(12L))),
                arguments(
                        Map.of(partition0, new OffsetAndMetadata(10L)),
                        Map.of(partition0, 5L, partition1, 12L),
                        Map.of(
                                partition0,
                                new OffsetAndMetadata(10L),
                                partition1,
                                new OffsetAndMetadata(12L))),
                arguments(
                        Map.of(partition0, new OffsetAndMetadata(2L)),
                        Map.of(partition0, 5L, partition1, 12L),
                        Map.of(
                                partition0,
                                new OffsetAndMetadata(2L),
                                partition1,
                                new OffsetAndMetadata(12L))),
                arguments(
                        Map.of(
                                partition0,
                                new OffsetAndMetadata(1L),
                                partition1,
                                new OffsetAndMetadata(7L)),
                        Map.of(partition0, 5L, partition1, 12L),
                        Map.of(
                                partition0,
                                new OffsetAndMetadata(1L),
                                partition1,
                                new OffsetAndMetadata(7L))));
    }

    @ParameterizedTest
    @MethodSource("initStoreArguments")
    void shouldInitStoreWithArguments(
            Map<TopicPartition, OffsetAndMetadata> committed,
            Map<TopicPartition, Long> startOffsets,
            Map<TopicPartition, OffsetAndMetadata> expected) {
        // Initialize the store
        offsetService.initStore(startOffsets, committed);

        // The store should have the expected initial committed repo
        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().snapshot();
        assertThat(map).isEqualTo(expected);
    }

    @Test
    void shouldInitStore() {
        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().snapshot();
        assertThat(map)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(10l),
                        partition1,
                        new OffsetAndMetadata(12L));
    }

    @Test
    void shouldUpdateOffsets() {
        // Initialize with a mocked store because we want to verify
        // just whether records are actually saved onto it upon invoking updateOffsets.
        offsetService.initStore(
                Mocks.MockOffsetStore::new,
                Map.of(partition0, 0L, partition1, 0L),
                Collections.emptyMap());
        MockOffsetStore store = (MockOffsetStore) offsetService.offsetStore().get();
        assertThat(store.getRecords()).isEmpty();

        ConsumerRecord<?, ?> record1 = ConsumerRecord(TOPIC, 0, "A-0");
        offsetService.updateOffsets(record1);
        ConsumerRecord<?, ?> record2 = ConsumerRecord(TOPIC, 0, "B-10");
        offsetService.updateOffsets(record2);
        assertThat(store.getRecords()).containsExactly(record1, record2);
    }

    @Test
    void shouldStoreFirstFailure() {
        // Notify two different exceptions
        ValueException firstFailure = ValueException.indexOfOutBounds(1);
        offsetService.onAsyncFailure(firstFailure);
        offsetService.onAsyncFailure(ValueException.noKeyFound("key"));
        // Verify that only the first one is stored
        assertThat(offsetService.getFirstFailure()).isSameInstanceAs(firstFailure);
    }

    @Test
    void shouldNotCommitSyncDueToException() {
        mockConsumer.setCommitException(new KafkaException("Simulated commit error"));
        KafkaException exception =
                assertThrows(KafkaException.class, () -> offsetService.commitSync());
        assertThat(exception).hasMessageThat().isEqualTo("Simulated commit error");
    }

    @Test
    void shouldCommitSync() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(record -> offsetService.updateOffsets(record));
        offsetService.commitSync();

        // Check the committed map
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    static Stream<Arguments> commitSyncAndIgnoreErrorsArguments() {
        return Stream.of(
                arguments(new KafkaException("Simulated commit error")),
                arguments(new RuntimeException("Generic error")));
    }

    @ParameterizedTest
    @MethodSource("commitSyncAndIgnoreErrorsArguments")
    void shouldCommitSyncAndIgnoreErrors(RuntimeException exception) {
        prepareCommittedRecords();

        // Configure the mock consumer to throw an exception on commit
        mockConsumer.setCommitException(exception);

        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(record -> offsetService.updateOffsets(record));
        offsetService.commitSyncAndIgnoreErrors();

        // Check the committed map has not changed
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterConsuming).isEqualTo(committedBeforeConsuming);
    }

    private void prepareCommittedRecords() {
        mockConsumer.commitSync(
                Map.of(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16l)));
    }

    @Test
    void shouldCommitAsync() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(record -> offsetService.updateOffsets(record));
        offsetService.commitAsync();

        // Check the committed map
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    @Test
    void shouldCommitOnPartitionRevoked() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(record -> offsetService.updateOffsets(record));

        // Trigger a rebalance, which in turn make the OffsetService invoke onPartitionsRevoked.
        // The rebalance event remove partition0.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition1)));
        records = mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(0);

        // Now the committed offsets should be populated
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));

        OffsetAndMetadata actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(0l);
        OffsetAndMetadata actualOnPartition1 = committed.get(partition1);
        assertThat(actualOnPartition1.offset()).isEqualTo(17l);
    }

    @Test
    void shouldIgnoreErrorsOnPartitionRevoked() {
        prepareCommittedRecords();

        // Configure the mock consumer to throw an exception on commit
        mockConsumer.setCommitException(new KafkaException("Simulated commit error"));

        // Normally, the store is initialized only after the very first poll invocation
        offsetService.initStore(false);

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(record -> offsetService.updateOffsets(record));

        // Trigger a rebalance, which in turn make the OffsetService invoke onPartitionsRevoked.
        // The rebalance event remove partition0.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition1)));
        records = mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(0);

        // Now the committed offsets should be populated
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));

        OffsetAndMetadata actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(0l);
        OffsetAndMetadata actualOnPartition1 = committed.get(partition1);
        // The offset on partition1 should be the last one consumed, as the commit was
        // ignored due to the exception.
        assertThat(actualOnPartition1.offset()).isEqualTo(16l);
    }

    @Test
    void shouldIdentifyPendingOffsets() {
        Map<TopicPartition, OffsetAndMetadata> committed =
                Map.of(partition0, new OffsetAndMetadata(10l, "11,14,20"));
        offsetService.initStore(Map.of(partition0, 0L, partition1, 0L), committed);

        // The following three records have their offsets stored in the metadata
        List<ConsumerRecord<?, ?>> havePendingOffsets =
                List.of(
                        ConsumerRecord(TOPIC, 0, "A-11"),
                        ConsumerRecord(TOPIC, 0, "A-14"),
                        ConsumerRecord(TOPIC, 0, "A-20"));

        for (ConsumerRecord<?, ?> record : havePendingOffsets) {
            assertThat(offsetService.notHasPendingOffset(record)).isFalse();
        }

        // The following records do not have their offsets stored as pending in the metadata
        List<ConsumerRecord<?, ?>> haveNoPendingOffsets =
                List.of(
                        ConsumerRecord(TOPIC, 0, "A-10"),
                        ConsumerRecord(TOPIC, 0, "A-12"),
                        ConsumerRecord(TOPIC, 0, "A-13"),
                        ConsumerRecord(TOPIC, 0, "A-15"),
                        ConsumerRecord(TOPIC, 0, "A-16"),
                        ConsumerRecord(TOPIC, 0, "A-17"),
                        ConsumerRecord(TOPIC, 0, "A-18"),
                        ConsumerRecord(TOPIC, 0, "A-19"),
                        ConsumerRecord(TOPIC, 0, "A-21"),
                        ConsumerRecord(TOPIC, 1, "B-21"));
        for (ConsumerRecord<?, ?> record : haveNoPendingOffsets) {
            assertThat(offsetService.notHasPendingOffset(record)).isTrue();
        }
    }

    @Test
    void shouldCommitWhenMaybeCommitThresholdReached() {
        prepareCommittedRecords();
        offsetService.initStore(false);

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Call maybeCommit with enough records to trigger fixed threshold (10,000)
        offsetService.maybeCommit(100_000);

        // Check that commit was triggered asynchronously
        Map<TopicPartition, OffsetAndMetadata> committedAfterMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Offsets should have been committed (advanced by 1 from consumed record offset)
        assertThat(committedAfterMaybeCommit)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));

        // Verify the committed offsets are different from before
        assertThat(committedAfterMaybeCommit).isNotEqualTo(committedBeforeMaybeCommit);
    }

    @Test
    void shouldNotCommitWhenMaybeCommitThresholdNotReached() {
        prepareCommittedRecords();
        offsetService.initStore(false);

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Call maybeCommit with insufficient records (below 10,000 threshold)
        offsetService.maybeCommit(5000);

        // Check that no commit was triggered
        Map<TopicPartition, OffsetAndMetadata> committedAfterMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Offsets should remain unchanged
        assertThat(committedAfterMaybeCommit).isEqualTo(committedBeforeMaybeCommit);
    }

    @Test
    void shouldAccumulateRecordCountsAcrossMultipleMaybeCommitCalls() {
        prepareCommittedRecords();
        offsetService.initStore(false);

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Call maybeCommit multiple times with small counts that accumulate to threshold
        offsetService.maybeCommit(30000); // Total: 30000
        offsetService.maybeCommit(40000); // Total: 70000
        offsetService.maybeCommit(20000); // Total: 90000 (still below threshold)

        // No commit should have happened yet
        Map<TopicPartition, OffsetAndMetadata> committedAfterPartialCalls =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterPartialCalls).isEqualTo(committedBeforeMaybeCommit);

        // One more call that pushes over threshold
        offsetService.maybeCommit(20000); // Total: 110000 (exceeds 10,000 threshold)

        // Now commit should have been triggered
        Map<TopicPartition, OffsetAndMetadata> committedAfterThresholdReached =
                mockConsumer.committed(Set.of(partition0, partition1));

        assertThat(committedAfterThresholdReached)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    @Test
    void shouldCommitWhenMaybeCommitTimeThresholdReached() throws InterruptedException {
        prepareCommittedRecords();
        offsetService.initStore(false);

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // First call to maybeCommit with small record count (below threshold)
        offsetService.maybeCommit(100);

        // Verify no commit happened yet (insufficient count and time)
        Map<TopicPartition, OffsetAndMetadata> committedAfterFirstCall =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterFirstCall).isEqualTo(committedBeforeMaybeCommit);

        // Wait for time threshold to be exceeded (FixedThresholdCommitStrategy uses 5 seconds)
        Thread.sleep(5100); // Sleep slightly over 5 seconds

        // Call maybeCommit again with small record count
        offsetService.maybeCommit(100);

        // Now commit should have been triggered due to time threshold
        Map<TopicPartition, OffsetAndMetadata> committedAfterTimeThreshold =
                mockConsumer.committed(Set.of(partition0, partition1));

        assertThat(committedAfterTimeThreshold)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    @Test
    void shouldResetCountersAfterCommit() {
        prepareCommittedRecords();
        offsetService.initStore(false);

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        // Trigger first commit by exceeding record threshold
        offsetService.maybeCommit(100000);

        // Verify commit happened
        Map<TopicPartition, OffsetAndMetadata> committedAfterFirstCommit =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterFirstCommit)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));

        // Add more records after the commit
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 0, "A-16"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) ConsumerRecord(TOPIC, 1, "B-17"));
                });

        records = mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(record -> offsetService.updateOffsets(record));

        // Call maybeCommit with small count (should not trigger commit since counters were reset)
        offsetService.maybeCommit(50000);

        // Verify no additional commit happened (counters were reset after previous commit)
        Map<TopicPartition, OffsetAndMetadata> committedAfterSecondCall =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterSecondCall).isEqualTo(committedAfterFirstCommit);

        // Now trigger another commit with sufficient accumulated count
        offsetService.maybeCommit(60000); // Total: 50000 + 60000 = 110000 (exceeds threshold)

        // Verify second commit happened with new offsets
        Map<TopicPartition, OffsetAndMetadata> committedAfterSecondCommit =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterSecondCommit)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(17L),
                        partition1,
                        new OffsetAndMetadata(18L));
    }
}
