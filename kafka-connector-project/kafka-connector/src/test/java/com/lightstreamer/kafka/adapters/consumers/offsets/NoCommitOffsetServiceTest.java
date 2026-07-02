
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

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class NoCommitOffsetServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(NoCommitOffsetServiceTest.class);

    private NoCommitOffsetService offsetService;

    @BeforeEach
    public void setUp() {
        offsetService = new NoCommitOffsetService(logger);
    }

    @Test
    public void shouldReturnEmptyOffsetsSnapshot() {
        assertThat(offsetService.offsetsSnapshot()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyOffsetsSnapshotAfterUpdateOffsets() {
        offsetService.updateOffsets(KafkaRecord.from("topic", 0, 42, 0L, "key", "value", null));
        assertThat(offsetService.offsetsSnapshot()).isEmpty();
    }

    @Test
    public void shouldReturnNullForGetFirstFailure() {
        assertThat(offsetService.getFirstFailure()).isNull();
    }

    @Test
    public void shouldReturnNullForGetFirstFailureAfterOnAsyncFailure() {
        offsetService.onAsyncFailure(new RuntimeException("test failure"));
        assertThat(offsetService.getFirstFailure()).isNull();
    }

    @Test
    public void shouldNotThrowOnMaybeCommit() {
        offsetService.maybeCommit();
        // No exception — verifies no-op behavior
    }

    @Test
    public void shouldNotThrowOnConsumerShutdown() {
        offsetService.onConsumerShutdown();
        // No exception — verifies no-op behavior
    }

    @Test
    public void shouldNotThrowOnPartitionsAssigned() {
        Set<TopicPartition> partitions =
                Set.of(new TopicPartition("topic", 0), new TopicPartition("topic", 1));
        offsetService.onPartitionsAssigned(partitions);
        // No exception — verifies graceful handling
    }

    @Test
    public void shouldNotThrowOnPartitionsRevoked() {
        List<TopicPartition> partitions = List.of(new TopicPartition("topic", 0));
        offsetService.onPartitionsRevoked(partitions);
        // No exception — verifies no commit attempt
    }

    @Test
    public void shouldNotThrowOnPartitionsLost() {
        List<TopicPartition> partitions = List.of(new TopicPartition("topic", 0));
        offsetService.onPartitionsLost(partitions);
        // No exception — verifies graceful handling
    }

    @Test
    public void shouldBeCreatableViaFactoryMethod() {
        OffsetService service = OffsetService.noCommit(logger);
        assertThat(service).isInstanceOf(NoCommitOffsetService.class);
        assertThat(service.offsetsSnapshot()).isEmpty();
        assertThat(service.getFirstFailure()).isNull();
    }
}
