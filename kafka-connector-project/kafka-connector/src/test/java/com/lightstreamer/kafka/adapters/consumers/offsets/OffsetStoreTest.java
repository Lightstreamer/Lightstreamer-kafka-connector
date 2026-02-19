
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

import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetStoreTest {

    private static String TEST_TOPIC = "topic";

    static KafkaRecord<?, ?> Record(int partition, String id) {
        return Records.KafkaRecord(TEST_TOPIC, partition, id);
    }

    private OffsetStore repo;

    OffsetAndMetadata getOffsetAndMetadata(String topic, int partition) {
        return repo.snapshot().get(new TopicPartition(topic, partition));
    }

    @Test
    void shouldReturnSameOffsetMap() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets);
        Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.snapshot();
        assertThat(offsetsMap).isEqualTo(offsets);
    }

    @Test
    void shouldUpdateOffsetSequentially() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0, null));

        repo = Offsets.OffsetStore(offsets);

        repo.save(Record(0, "A-0"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o1.offset()).isEqualTo(1);
        assertThat(o1.metadata()).isEqualTo("");

        repo.save(Record(0, "B-1"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o2.offset()).isEqualTo(2);
        assertThat(o2.metadata()).isEqualTo("");
    }

    @Test
    void shouldNotUpdateOutOfOrderOffset() {
        long LAST_COMMITTED_OFFSET = 10;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets);

        repo.save(Record(0, "A-2"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);

        // Last offset is still the one provided by the initializing map (LAST_COMMITTED_OFFSET)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEmpty();

        // Now provide a record with the next expected offset
        repo.save(Record(0, "A-10"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);

        // Last offset is advanced as expected
        assertThat(o2.offset()).isEqualTo(11);
        assertThat(o2.metadata()).isEmpty();
    }

    @Test
    void shouldReturnSnapshot() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));
        offsets.put(new TopicPartition(TEST_TOPIC, 1), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets);
        assertThat(repo.snapshot()).isEqualTo(offsets);

        repo.save(Record(0, "A-2"));
        repo.save(Record(1, "B-3"));

        Map<TopicPartition, OffsetAndMetadata> snapshot = repo.snapshot();
        OffsetAndMetadata o1 = snapshot.get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(3);
        assertThat(o1.metadata()).isEmpty();

        OffsetAndMetadata o2 = snapshot.get(new TopicPartition(TEST_TOPIC, 1));
        assertThat(o2.offset()).isEqualTo(4);
        assertThat(o2.metadata()).isEmpty();
    }

    @Test
    public void shouldClearPartitions() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));
        offsets.put(new TopicPartition(TEST_TOPIC, 1), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets);

        repo.save(Record(0, "A-1"));
        repo.save(Record(1, "B-1"));

        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o1.offset()).isEqualTo(2);
        assertThat(o1.metadata()).isEqualTo("");

        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 1);
        assertThat(o2.offset()).isEqualTo(2);
        assertThat(o2.metadata()).isEqualTo("");

        // Clear only partition 0
        repo.clear(List.of(new TopicPartition(TEST_TOPIC, 0)));

        // Verify partition 0 cleared
        OffsetAndMetadata o3 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o3).isNull();

        // Verify partition 1 still present
        OffsetAndMetadata o4 = getOffsetAndMetadata(TEST_TOPIC, 1);
        assertThat(o4.offset()).isEqualTo(2);
        assertThat(o4.metadata()).isEqualTo("");
    }
}
