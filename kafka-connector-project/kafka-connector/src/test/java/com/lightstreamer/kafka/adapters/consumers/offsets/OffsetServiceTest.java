
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

import com.lightstreamer.kafka.adapters.consumers.Fakes;
import com.lightstreamer.kafka.adapters.consumers.Fakes.FakeOffsetStore;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OffsetServiceTest {

    private static final String TOPIC = "topic";

    private MockConsumer<?, ?> mockConsumer;
    private OffsetService offsetService;
    private FakeOffsetStore fakeOffsetStore;

    @BeforeEach
    void beforeEach() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        // Set start offsets for the partition
        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(new TopicPartition(TOPIC, 0), 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);

        offsetService = OffsetService.newOffsetService(mockConsumer);

        Map<TopicPartition, OffsetAndMetadata> topicMap = new HashMap<>();
        topicMap.put(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(10l));
        topicMap.put(new TopicPartition(TOPIC, 1), new OffsetAndMetadata(12l));
        fakeOffsetStore = new Fakes.FakeOffsetStore(topicMap);
        offsetService.initStore(true, map -> fakeOffsetStore);
    }

    @Test
    void shouldUpdateOffsets() {
        ConsumerRecord<?, ?> record1 = ConsumerRecords.Record(TOPIC, 0, "0A");
        offsetService.updateOffsets(record1);
        ConsumerRecord<?, ?> record2 = ConsumerRecords.Record(TOPIC, 0, "0B");
        offsetService.updateOffsets(record2);
        assertThat(fakeOffsetStore.getRecords()).containsExactly(record1, record2);
    }

    @Test
    void shouldStoreFirstFailure() {
        ValueException firstFailure = ValueException.indexOfOutBoundex(1);
        offsetService.onAsyncFailure(firstFailure);
        offsetService.onAsyncFailure(ValueException.noKeyFound("key"));
        assertThat(offsetService.getFirstFailure()).isSameInstanceAs(firstFailure);
    }

    void shouldEvaluteIfRecordCanBeProcessed() {}

    @Test
    void shouldCommitySync() {
        offsetService.commitSync();
        Map<TopicPartition, OffsetAndMetadata> committed =
                mockConsumer.committed(Collections.singleton(new TopicPartition(TOPIC, 0)));
        OffsetAndMetadata actual = committed.get(new TopicPartition(TOPIC, 0));
        assertThat(actual.offset()).isEqualTo(10l);
    }
}
