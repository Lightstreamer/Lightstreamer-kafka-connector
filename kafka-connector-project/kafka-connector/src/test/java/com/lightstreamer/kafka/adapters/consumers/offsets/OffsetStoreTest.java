
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    void shouldDecode() {
        assertThat(Offsets.decode("")).isEmpty();
        assertThat(Offsets.decode(",")).isEmpty();
        assertThat(Offsets.decode("200")).containsExactly(200l);
        assertThat(Offsets.decode("200,100,400")).containsExactly(200l, 100l, 400l);
        assertThat(Offsets.decode("200,100,400")).isInOrder();
    }

    @Test
    void shouldAppend() {
        assertThat(Offsets.append("", 100)).isEqualTo("100");
        assertThat(Offsets.append("100,300,150,301", 271)).isEqualTo("100,300,150,301,271");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReturnSameOffsetMap(boolean manageHoles) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets, manageHoles);
        Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.snapshot();
        assertThat(offsetsMap).isEqualTo(offsets);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldUpdateOffsetSequentially(boolean manageHoles) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0, null));

        repo = Offsets.OffsetStore(offsets, manageHoles);

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
    void shouldUpdateOffsetBeyondLastCommitted() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-2"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEqualTo("2");

        repo.save(Record(0, "B-4"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o2.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o2.metadata()).isEqualTo("2,4");

        repo.save(Record(0, "B-6"));
        OffsetAndMetadata o3 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o3.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o3.metadata()).isEqualTo("2,4,6");
    }

    @Test
    void shouldNotUpdateOffsetBeyondLastCommitted() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, false);

        repo.save(Record(0, "A-2"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(3);
        assertThat(o1.metadata()).isEmpty();

        repo.save(Record(0, "B-4"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o2.offset()).isEqualTo(5);
        assertThat(o2.metadata()).isEmpty();

        repo.save(Record(0, "B-6"));
        OffsetAndMetadata o3 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o3.offset()).isEqualTo(7);
        assertThat(o3.metadata()).isEmpty();
    }

    @Test
    void shouldSanitizeBeyondOffsetsSimple() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-1"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEqualTo("1");

        // Save the record whose offset matched the LAST_COMMITTED_OFFSET
        repo.save(Record(0, "A-0"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Sanitization happened
        assertThat(o2.offset()).isEqualTo(2);
        assertThat(o2.metadata()).isEqualTo("");
    }

    @Test
    void shouldSanitizeBeyondOffsetsComplex() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-1"));
        repo.save(Record(0, "A-2"));
        repo.save(Record(0, "A-3"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEqualTo("1,2,3");

        repo.save(Record(0, "A-0"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // After committing the record whose offset matched the LAST_COMMITTED_OFFSET,
        assertThat(o2.offset()).isEqualTo(4);
        assertThat(o2.metadata()).isEmpty();
    }

    @Test
    void shouldSanitizeBeyondOffsetsComplexUnordered() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-300"));
        repo.save(Record(0, "A-100"));
        repo.save(Record(0, "A-200"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEqualTo("300,100,200");

        repo.save(Record(0, "A-0"));
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // After committing the record whose offset matched the LAST_COMMITTED_OFFSET,
        // the consumed list
        assertThat(o2.offset()).isEqualTo(1);
        assertThat(o2.metadata()).isEqualTo("300,100,200");
    }

    @Test
    void shouldSanitizeBeyondOffsetsVeryComplex() {
        long LAST_COMMITTED_OFFSET = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(LAST_COMMITTED_OFFSET));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-5"));
        repo.save(Record(0, "A-1"));
        repo.save(Record(0, "A-2"));
        repo.save(Record(0, "A-3"));
        repo.save(Record(0, "A-6"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        // Last offset is still the one provided by the initializing map (0)
        assertThat(o1.offset()).isEqualTo(LAST_COMMITTED_OFFSET);
        assertThat(o1.metadata()).isEqualTo("5,1,2,3,6");

        repo.save(Record(0, "A-0"));
        // Now the consumed list has been sanitized from 0 to 3, the next offset is 4
        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o2.offset()).isEqualTo(4);
        assertThat(o2.metadata()).isEqualTo("5,6");

        repo.save(Record(0, "A-4"));
        // Sanitization complete
        OffsetAndMetadata o3 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o3.offset()).isEqualTo(7);
        assertThat(o3.metadata()).isEqualTo("");
    }

    @Test
    void shouldReturnSnapshot() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets, true);
        assertThat(repo.snapshot()).isEqualTo(offsets);

        repo.save(Record(0, "A-1"));
        OffsetAndMetadata o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(0);
        assertThat(o1.metadata()).isEqualTo("1");

        repo.save(Record(0, "A-0"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(2);
        assertThat(o1.metadata()).isEqualTo("");

        repo.save(Record(0, "A-2"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(3);
        assertThat(o1.metadata()).isEqualTo("");

        repo.save(Record(0, "A-4"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(3);
        assertThat(o1.metadata()).isEqualTo("4");
        repo.save(Record(0, "A-5"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(3);
        assertThat(o1.metadata()).isEqualTo("4,5");

        repo.save(Record(0, "A-3"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(6);
        assertThat(o1.metadata()).isEqualTo("");

        repo.save(Record(0, "A-8"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(6);
        assertThat(o1.metadata()).isEqualTo("8");

        repo.save(Record(0, "A-9"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(6);
        assertThat(o1.metadata()).isEqualTo("8,9");

        repo.save(Record(0, "A-6"));
        o1 = repo.snapshot().get(new TopicPartition(TEST_TOPIC, 0));
        assertThat(o1.offset()).isEqualTo(7);
        assertThat(o1.metadata()).isEqualTo("8,9");
    }

    @Test
    public void shouldClear() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-1"));
        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o1.offset()).isEqualTo(0);
        assertThat(o1.metadata()).isEqualTo("1");

        repo.clear();

        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o2).isNull();
    }

    @Test
    public void shouldClearPartitions() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TEST_TOPIC, 0), new OffsetAndMetadata(0));
        offsets.put(new TopicPartition(TEST_TOPIC, 1), new OffsetAndMetadata(0));

        repo = Offsets.OffsetStore(offsets, true);

        repo.save(Record(0, "A-1"));
        repo.save(Record(1, "B-1"));

        OffsetAndMetadata o1 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o1.offset()).isEqualTo(0);
        assertThat(o1.metadata()).isEqualTo("1");

        OffsetAndMetadata o2 = getOffsetAndMetadata(TEST_TOPIC, 1);
        assertThat(o2.offset()).isEqualTo(0);
        assertThat(o2.metadata()).isEqualTo("1");

        repo.clear(List.of(new TopicPartition(TEST_TOPIC, 0)));

        OffsetAndMetadata o3 = getOffsetAndMetadata(TEST_TOPIC, 0);
        assertThat(o3).isNull();

        OffsetAndMetadata o4 = getOffsetAndMetadata(TEST_TOPIC, 1);
        assertThat(o4.offset()).isEqualTo(0);
        assertThat(o4.metadata()).isEqualTo("1");
    }
}
