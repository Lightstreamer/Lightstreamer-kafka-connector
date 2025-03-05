
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
import static com.lightstreamer.kafka.test_utils.Records.Record;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetStore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        setUpFromEarliast();
        // setUpFromLatest();
    }

    void setUpFromEarliast() {
        setUp(true);
    }

    void setUpFromLatest() {
        setUp(false);
    }

    private void setUp(boolean earliest) {
        this.mockConsumer = new MockConsumer<>(earliest ? EARLIEST : LATEST);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

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
        this.offsetService = Offsets.OffsetService(mockConsumer);

        // Subscribe to topic specifying the OffsetService as ConsumerRebalancerListener
        mockConsumer.subscribe(Collections.singleton(TOPIC), offsetService);

        // A first poll is required to trigger partitions assignment.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
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
        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().current();
        assertThat(map).isEqualTo(expected);
    }

    @Test
    void shouldInitStore() {
        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false);

        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().current();
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

        ConsumerRecord<?, ?> record1 = Record(TOPIC, 0, "A-0");
        offsetService.updateOffsets(record1);
        ConsumerRecord<?, ?> record2 = Record(TOPIC, 0, "B-10");
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

    @SuppressWarnings("unchecked")
    @Test
    void shouldCommitySync() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false);

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets and then commit
        records.forEach(record -> offsetService.updateOffsets(record));
        offsetService.commitSync();

        // Check the committed map
        committed = mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    private void prepareCommittedRecords() {
        mockConsumer.commitSync(
                Map.of(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16l)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCommityAsync() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false);

        // No commit happens before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(15L),
                        partition1,
                        new OffsetAndMetadata(16L));

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsetns and then commit
        records.forEach(record -> offsetService.updateOffsets(record));
        offsetService.commitAsync();

        // Check the committed map
        committed = mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(16L),
                        partition1,
                        new OffsetAndMetadata(17L));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCommitOnPartitionRevoked() {
        prepareCommittedRecords();

        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false);

        // Poll two new records
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 0, "A-15"));
                    mockConsumer.addRecord(
                            (ConsumerRecord<String, String>) Record(TOPIC, 1, "B-16"));
                });
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(2);

        // Update the offsets BUT NOT commit
        records.forEach(record -> offsetService.updateOffsets(record));

        // Trigger a rebalance, which in turn make the OffsetService invoke onPartitionsRevoked
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
    void shouldIdentifyPendingOffsets() {
        Map<TopicPartition, OffsetAndMetadata> committed =
                Map.of(partition0, new OffsetAndMetadata(10l, "11,14,20"));
        offsetService.initStore(Map.of(partition0, 0L, partition1, 0L), committed);

        // The following three records have their offset stored in the metadata
        List<ConsumerRecord<?, ?>> havePendingOffsets =
                List.of(
                        Record(TOPIC, 0, "A-11"),
                        Record(TOPIC, 0, "A-14"),
                        Record(TOPIC, 0, "A-20"));

        for (ConsumerRecord<?, ?> record : havePendingOffsets) {
            assertThat(offsetService.notHasPendingOffset(record)).isFalse();
        }

        // The following records do not have their offsets stored as pending in the metadata
        List<ConsumerRecord<?, ?>> dontHavePendingOffsets =
                List.of(
                        Record(TOPIC, 0, "A-10"),
                        Record(TOPIC, 0, "A-12"),
                        Record(TOPIC, 0, "A-13"),
                        Record(TOPIC, 0, "A-15"),
                        Record(TOPIC, 0, "A-16"),
                        Record(TOPIC, 0, "A-17"),
                        Record(TOPIC, 0, "A-18"),
                        Record(TOPIC, 0, "A-19"),
                        Record(TOPIC, 0, "A-21"),
                        Record(TOPIC, 1, "B-21"));
        for (ConsumerRecord<?, ?> record : dontHavePendingOffsets) {
            assertThat(offsetService.notHasPendingOffset(record)).isTrue();
        }
    }
}
