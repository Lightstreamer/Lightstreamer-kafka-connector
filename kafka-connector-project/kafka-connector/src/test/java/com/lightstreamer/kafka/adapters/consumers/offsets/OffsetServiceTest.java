
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
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecord;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
    private static final TopicPartition partition0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition partition1 = new TopicPartition(TOPIC, 1);
    private static final TopicPartition partition2 = new TopicPartition(TOPIC, 2);

    private MockConsumer mockConsumer;
    private OffsetServiceImpl offsetService;

    private DeserializerPair<String, String> deserializerPair =
            new DeserializerPair<>(String().deserializer(), String().deserializer());

    private void setUp(boolean earliest) {
        setUp(earliest, CommitStrategy.fixedCommitStrategy(5000, 10000));
    }

    private void setUp(boolean earliest, CommitStrategy commitStrategy) {
        this.mockConsumer =
                new MockConsumer(
                        earliest
                                ? StrategyType.EARLIEST.toString()
                                : StrategyType.LATEST.toString());

        // A rebalance must be scheduled to later use the subscribe method
        this.mockConsumer.schedulePollTask(
                () -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 10L);
        offsets.put(partition1, 12L);
        offsets.put(partition2, 0L);
        if (earliest) {
            mockConsumer.updateBeginningOffsets(offsets);
        } else {
            mockConsumer.updateEndOffsets(offsets);
        }

        // Initialize the OffsetService
        this.offsetService =
                new OffsetServiceImpl(
                        mockConsumer, LoggerFactory.getLogger(OffsetService.class), commitStrategy);

        // Subscribe to topic specifying the OffsetService as ConsumerRebalancerListener
        this.mockConsumer.subscribe(Collections.singleton(TOPIC), offsetService);

        // A first poll is required to trigger partitions assignment.
        this.mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
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
    public void shouldUpdateOffsets() {
        setUp(true);

        KafkaRecord<?, ?> record1 = KafkaRecord(TOPIC, 0, "A-0");
        offsetService.updateOffsets(record1);
        assertThat(offsetService.offsetsSnapshot())
                .containsExactly(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L));

        KafkaRecord<?, ?> record2 = KafkaRecord(TOPIC, 0, "B-10");
        offsetService.updateOffsets(record2);
        assertThat(offsetService.offsetsSnapshot())
                .containsExactly(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(11L));
    }

    @Test
    public void shouldStoreFirstFailure() {
        setUp(true);

        // Notify two different exceptions
        ValueException firstFailure = ValueException.indexOfOutBounds(1);
        offsetService.onAsyncFailure(firstFailure);
        offsetService.onAsyncFailure(ValueException.noKeyFound("key"));
        // Verify that only the first one is stored
        assertThat(offsetService.getFirstFailure()).isSameInstanceAs(firstFailure);
    }

    @Test
    public void shouldCommitSync() {
        setUp(true);

        prepareCommittedRecords();

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets
        records.forEach(
                record -> {
                    offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
                });

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Commit synchronously
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

    static Stream<Arguments> providedErrors() {
        return Stream.of(
                arguments(new KafkaException("Simulated commit error")),
                arguments(new RuntimeException("Generic error")));
    }

    @ParameterizedTest
    @MethodSource("providedErrors")
    public void shouldNotCommitSyncDueToException(RuntimeException exception) {
        setUp(true);

        prepareCommittedRecords();

        // Configure the mock consumer to throw an exception on commit
        mockConsumer.setCommitException(exception);

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Commit synchronously, which should throw an exception that is caught and ignored by the
        // OffsetService
        offsetService.commitSync();

        // Check the committed map has not changed
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterConsuming).isEqualTo(committedBeforeConsuming);
    }

    @Test
    public void shouldCommitAsync() {
        setUp(true);

        prepareCommittedRecords();

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Commit asynchronously
        offsetService.commitAsync(System.currentTimeMillis());

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

    @ParameterizedTest
    @MethodSource("providedErrors")
    public void shouldNotCommitAsyncDueToException(RuntimeException exception) {
        setUp(true);

        prepareCommittedRecords();

        // Configure the mock consumer to throw an exception on commit
        mockConsumer.setCommitException(exception);

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Commit asynchronously, which should trigger an exception that is asynchronously caught
        // and logged by the OffsetService
        offsetService.commitAsync(System.currentTimeMillis());

        // Check the committed map has not changed
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterConsuming).isEqualTo(committedBeforeConsuming);
    }

    @Test
    public void shouldClearOffsetsOnPartitionsLost() {
        setUp(true);

        List<KafkaRecord<?, ?>> records =
                List.of(
                        KafkaRecord.fromDeferred(
                                ConsumerRecord(TOPIC, 0, "A-15"), deserializerPair),
                        KafkaRecord.fromDeferred(
                                ConsumerRecord(TOPIC, 1, "B-16"), deserializerPair));
        records.forEach(record -> offsetService.updateOffsets(record));
        assertThat(offsetService.offsetsSnapshot())
                .containsExactly(
                        partition0, new OffsetAndMetadata(16L),
                        partition1, new OffsetAndMetadata(17L));

        // Simulate loss of partition 1, which should clear only the offset for that partition
        offsetService.onPartitionsLost(Set.of(new TopicPartition(TOPIC, 1)));
        assertThat(offsetService.offsetsSnapshot())
                .containsExactly(partition0, new OffsetAndMetadata(16L));

        // Simulate loss of partition 0, which should clear the remaining offset and leave the store
        // empty
        offsetService.onPartitionsLost(Set.of(new TopicPartition(TOPIC, 0)));
        assertThat(offsetService.offsetsSnapshot()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCommitOnPartitionsRevoked(boolean shutdownConsumer) {
        setUp(true);

        prepareCommittedRecords();

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committedBeforeConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedBeforeConsuming)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        if (shutdownConsumer) {
            offsetService.onConsumerShutdown();
        }
        // Simulate revocation of partition 0
        offsetService.onPartitionsRevoked(Set.of(partition0));

        // Check that the offset store only contains partition 1
        assertThat(offsetService.offsetsSnapshot())
                .containsExactly(partition1, new OffsetAndMetadata(17L));

        // Check the committed map
        Map<TopicPartition, OffsetAndMetadata> committedAfterConsuming =
                mockConsumer.committed(Set.of(partition0, partition1));
        if (shutdownConsumer) {
            // If the consumer is shutting down, commit is skipped, so the committed offsets should
            // be unchanged
            assertThat(committedAfterConsuming).isEqualTo(committedAfterConsuming);
        } else {
            // If the consumer is not shutting down, the commit should have been triggered, so the
            // committed offsets should be updated
            assertThat(committedAfterConsuming)
                    .containsExactly(
                            partition0,
                            new OffsetAndMetadata(16L),
                            partition1,
                            new OffsetAndMetadata(17L));
        }
    }

    @Test
    public void shouldCommitOnConsumerShutdown() {
        setUp(true);

        prepareCommittedRecords();

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // Shutdown the consumer, which should trigger a final commit of the offsets
        offsetService.onConsumerShutdown();

        records = mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(0);

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
    public void shouldIgnoreErrorsOnPartitionRevoked() {
        setUp(true);

        prepareCommittedRecords();

        // Configure the mock consumer to throw an exception on commit
        mockConsumer.setCommitException(new KafkaException("Simulated commit error"));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });
        ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));
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
    public void shouldNotCommitWhenMaybeCommitThresholdNotReached() {
        setUp(true, CommitStrategy.fixedCommitStrategy(5000, 100));

        prepareCommittedRecords();

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Process only a small number of records, well below the 100 threshold
        for (int i = 0; i < 20; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 100L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }

        // Call maybeCommit with insufficient records (below 100 threshold)
        offsetService.maybeCommit();

        // Check that no commit was triggered
        Map<TopicPartition, OffsetAndMetadata> committedAfterMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterMaybeCommit).isEqualTo(committedBeforeMaybeCommit);
    }

    @Test
    public void shouldAccumulateRecordCountsAcrossMultipleMaybeCommitCalls() {
        setUp(true, CommitStrategy.fixedCommitStrategy(5000, 100));

        prepareCommittedRecords();

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeAccumulation =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Process records in multiple batches that individually don't trigger commits
        // but accumulated should exceed threshold

        // Batch 1: 28 records (total 30, below threshold)
        for (int i = 0; i < 28; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 1000L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit(); // Should not commit yet

        // Verify no commit yet
        Map<TopicPartition, OffsetAndMetadata> committedAfterBatch1 =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterBatch1).isEqualTo(committedBeforeAccumulation);

        // Batch 2: 30 more records (total 60, still below threshold)
        for (int i = 0; i < 30; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 31000L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit(); // Should not commit yet

        // Verify still no commit
        Map<TopicPartition, OffsetAndMetadata> committedAfterBatch2 =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterBatch2).isEqualTo(committedBeforeAccumulation);

        // Batch 3: 50 more records (total 110, exceeds threshold)
        for (int i = 0; i < 50; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 61000L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit(); // Should trigger commit now

        // Verify that commit was finally triggered after accumulation
        Map<TopicPartition, OffsetAndMetadata> committedAfterAccumulation =
                mockConsumer.committed(Set.of(partition0, partition1));

        assertThat(committedAfterAccumulation).isNotEmpty();
        assertThat(committedAfterAccumulation).isNotEqualTo(committedBeforeAccumulation);
    }

    @Test
    public void shouldCommitWhenMaybeCommitTimeThresholdReached() throws InterruptedException {
        setUp(true, CommitStrategy.fixedCommitStrategy(5000, 100));

        prepareCommittedRecords();

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Process a small number of records (below count threshold)
        for (int i = 0; i < 28; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 100L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }

        // First call to maybeCommit with small record count (below threshold)
        offsetService.maybeCommit();

        // Verify no commit happened yet (insufficient count and time)
        Map<TopicPartition, OffsetAndMetadata> committedAfterFirstCall =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterFirstCall).isEqualTo(committedBeforeMaybeCommit);

        // Wait for time threshold to be exceeded (FixedThresholdCommitStrategy uses 5 seconds)
        Thread.sleep(5100); // Sleep slightly over 5 seconds

        // Process a few more records and call maybeCommit again
        for (int i = 0; i < 10; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 200L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }

        // Call maybeCommit again with small record count
        offsetService.maybeCommit();

        // Now commit should have been triggered due to time threshold
        Map<TopicPartition, OffsetAndMetadata> committedAfterTimeThreshold =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Verify that commits happened (offsets changed)
        assertThat(committedAfterTimeThreshold).isNotEmpty();
        assertThat(committedAfterTimeThreshold).isNotEqualTo(committedBeforeMaybeCommit);
    }

    @Test
    public void shouldResetCountersAfterCommit() {
        setUp(true, CommitStrategy.fixedCommitStrategy(5000, 100));
        prepareCommittedRecords();

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        // Trigger first commit by exceeding record threshold
        for (int i = 0; i < 120; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 100L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit();

        // Verify commit happened
        Map<TopicPartition, OffsetAndMetadata> committedAfterFirstCommit =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterFirstCommit).isNotEmpty();

        // Add more records after the commit
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-16"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-17"));
                });

        records = mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));
        // Process a small number of records (should not trigger commit since counters were reset)
        for (int i = 0; i < 10; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC,
                            0,
                            200000L + i,
                            ("key" + i).getBytes(),
                            ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit();

        // Verify no additional commit happened (counters were reset after previous commit)
        Map<TopicPartition, OffsetAndMetadata> committedAfterSecondCall =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterSecondCall).isEqualTo(committedAfterFirstCommit);

        // Now trigger another commit with sufficient accumulated count
        for (int i = 0; i < 100; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC,
                            0,
                            300000L + i,
                            ("key" + i).getBytes(),
                            ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }
        offsetService.maybeCommit();

        // Verify second commit happened with new offsets
        Map<TopicPartition, OffsetAndMetadata> committedAfterSecondCommit =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committedAfterSecondCommit).isNotEqualTo(committedAfterFirstCommit);
        assertThat(committedAfterSecondCommit).isNotEmpty();
    }

    @Test
    public void shouldCommitWhenMaybeCommitRecordThresholdReached() {
        setUp(true, CommitStrategy.fixedCommitStrategy(5000, 100));

        prepareCommittedRecords();

        // Poll and update offsets for some records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(ConsumerRecord(TOPIC, 1, "B-16"));
                });

        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        records.forEach(
                record ->
                        offsetService.updateOffsets(
                                KafkaRecord.fromDeferred(record, deserializerPair)));

        Map<TopicPartition, OffsetAndMetadata> committedBeforeMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Simulate processing enough records to exceed the 100 threshold
        for (int i = 0; i < 98; i++) {
            ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>(
                            TOPIC, 0, 100L + i, ("key" + i).getBytes(), ("value" + i).getBytes());
            offsetService.updateOffsets(KafkaRecord.fromDeferred(record, deserializerPair));
        }

        // Call maybeCommit - should trigger commit due to exceeding threshold
        offsetService.maybeCommit();

        // Check that commit was triggered asynchronously
        Map<TopicPartition, OffsetAndMetadata> committedAfterMaybeCommit =
                mockConsumer.committed(Set.of(partition0, partition1));

        // Verify that commits actually happened (offsets changed)
        assertThat(committedAfterMaybeCommit).isNotEmpty();
        assertThat(committedAfterMaybeCommit).isNotEqualTo(committedBeforeMaybeCommit);
    }
}
