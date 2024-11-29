
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

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.Mocks;
import com.lightstreamer.kafka.adapters.Mocks.MockOffsetStore;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Records;

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

    private MockConsumer<?, ?> mockConsumer;
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
        mockConsumer.commitAsync();
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
    void shouldInitStore(
            Map<TopicPartition, OffsetAndMetadata> committed,
            Map<TopicPartition, Long> startOffset,
            Map<TopicPartition, OffsetAndMetadata> expected) {
        // Initialize the store
        offsetService.initStore(Mocks.MockOffsetStore::new, startOffset, committed);

        // The store should have the expected initial committed repo
        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().current();
        assertThat(map).isEqualTo(expected);
    }

    @Test
    void shouldUpdateOffsets() {
        offsetService.initStore(
                Mocks.MockOffsetStore::new,
                Map.of(partition0, 0L, partition1, 0L),
                Collections.emptyMap());
        MockOffsetStore store = (MockOffsetStore) offsetService.offsetStore().get();
        assertThat(store.getRecords()).isEmpty();

        ConsumerRecord<?, ?> record1 = Records.Record(TOPIC, 0, "0A");
        offsetService.updateOffsets(record1);
        ConsumerRecord<?, ?> record2 = Records.Record(TOPIC, 0, "0B");
        offsetService.updateOffsets(record2);
        assertThat(store.getRecords()).containsExactly(record1, record2);
    }

    @Test
    void shouldStoreFirstFailure() {
        ValueException firstFailure = ValueException.indexOfOutBoundex(1);
        offsetService.onAsyncFailure(firstFailure);
        offsetService.onAsyncFailure(ValueException.noKeyFound("key"));
        assertThat(offsetService.getFirstFailure()).isSameInstanceAs(firstFailure);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCommitySync() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(EARLIEST);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 10L);
        offsets.put(partition1, 0L);
        mockConsumer.updateBeginningOffsets(offsets);

        // Initialize the OffsetService
        OffsetService offsetService = Offsets.OffsetService(mockConsumer);

        // Subscribe to topic specifying the OffsetService as ConsumerRebalancerListener
        mockConsumer.subscribe(Collections.singleton(TOPIC), offsetService);
        assertThat(mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE)).count()).isEqualTo(0);
        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false);

        Map<TopicPartition, OffsetAndMetadata> map = offsetService.offsetStore().get().current();
        assertThat(map)
                .containsExactly(
                        partition0,
                        new OffsetAndMetadata(10l),
                        partition1,
                        new OffsetAndMetadata(0L));
        // No committed offsets before invocation of commitSync
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed).isEmpty();

        // Commit
        offsetService.commitSync();

        // Now the committed offsets should be populated
        committed = mockConsumer.committed(Set.of(partition0, partition1));
        OffsetAndMetadata actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(10l);

        OffsetAndMetadata actualOnPartition1 = committed.get(partition1);
        assertThat(actualOnPartition1.offset()).isEqualTo(0L);

        mockConsumer.schedulePollTask(
                () ->
                        mockConsumer.addRecord(
                                (ConsumerRecord<String, String>) Records.Record(TOPIC, 0, "10A")));
        org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(records.count()).isEqualTo(1);

        // Commit
        List<ConsumerRecord<String, String>> records2 = records.records(partition0);
        offsetService.updateOffsets(records2.get(0));
        offsetService.commitSync();
        committed = mockConsumer.committed(Set.of(partition0, partition1));
        actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(11l);
    }

    @Test
    void shouldCommiytOnPartitionRevoked() {
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false, Mocks.MockOffsetStore::new);

        // No committed offsets before invocation of onPartitionsRevoked
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));
        assertThat(committed).isEmpty();

        // Revoke partitions
        offsetService.onPartitionsRevoked(Set.of(partition1));

        // Now the committed offsets should be populated
        committed = mockConsumer.committed(Set.of(partition0, partition1));

        OffsetAndMetadata actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(10l);

        OffsetAndMetadata actualOnPartition1 = committed.get(partition1);
        assertThat(actualOnPartition1.offset()).isEqualTo(12l);
    }

    @Test
    void shouldCommityAsync() {
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        // Normally, the store is initialized only after the very firt poll invocation
        offsetService.initStore(false, Mocks.MockOffsetStore::new);

        // Commit and test the offsets
        offsetService.commitAsync();
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Set.of(partition0, partition1));

        OffsetAndMetadata actualOnPartition0 = committed.get(partition0);
        assertThat(actualOnPartition0.offset()).isEqualTo(10l);

        OffsetAndMetadata actualOnPartition1 = committed.get(partition1);
        assertThat(actualOnPartition1.offset()).isEqualTo(12l);
    }

    void shouldEvaluteIfRecordCanBeProcessed() {}
}
