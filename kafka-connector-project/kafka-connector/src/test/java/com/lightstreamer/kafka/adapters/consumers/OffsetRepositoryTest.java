
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

package com.lightstreamer.kafka.adapters.consumers;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.consumers.OffsetRepository.OffsetEncoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class OffsetRepositoryTest {

    private OffsetRepository repo;
    private static String TOPIC = "topic";

    @BeforeEach
    void beforeEcach() {
        repo = new OffsetRepository();
    }

    // @Test
    // void shouldReturnOffsetMap() {
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 1, 1, "val-1"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 2, 1, "val-2"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 3, 1, "val-3"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 5, 1, "val-5"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 7, 1, "val-7"));

    //     // First failued record at offset 4
    //     repo.markFailureRecord(new ConsumerRecord<>(TOPIC, 0, 4, 1, "val-4"));
    //     // Other failed records beyond offeet 4
    //     repo.markFailureRecord(new ConsumerRecord<>(TOPIC, 0, 6, 1, "val-6"));
    //     repo.markFailureRecord(new ConsumerRecord<>(TOPIC, 0, 8, 1, "val-8"));

    //     Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.toOffsetsMap();
    //     assertThat(offsetsMap).hasSize(1);
    //     OffsetAndMetadata o = offsetsMap.get(new TopicPartition(TOPIC, 0));
    //     assertThat(o.offset()).isEqualTo(4);

    //     assertThat(OffsetEncoder.decode(o.metadata())).containsExactly(5l, 7l);
    // }

    // @Test
    // void shouldReturnOffsetMap2() {
    //     repo.markFailureRecord(new ConsumerRecord<>(TOPIC, 0, 0, 1, "val-0"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 1, 1, "val-1"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 2, 1, "val-2"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 3, 1, "val-3"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 4, 1, "val-4"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 5, 1, "val-5"));

    //     Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.toOffsetsMap();
    //     assertThat(offsetsMap).hasSize(1);
    //     OffsetAndMetadata o = offsetsMap.get(new TopicPartition(TOPIC, 0));
    //     assertThat(o.offset()).isEqualTo(0);

    //     assertThat(OffsetEncoder.decode(o.metadata())).containsExactly(1l, 2l, 3l, 4l, 5l);
    // }

    // @Test
    // void shouldReturnOffsetMap3() {
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 0, 1, "val-0"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 1, 1, "val-1"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 2, 1, "val-2"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 3, 1, "val-3"));
    //     repo.markSuccessRecord(new ConsumerRecord<>(TOPIC, 0, 4, 1, "val-4"));
    //     repo.markFailureRecord(new ConsumerRecord<>(TOPIC, 0, 5, 1, "val-5"));

    //     Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.toOffsetsMap();
    //     assertThat(offsetsMap).hasSize(1);
    //     OffsetAndMetadata o = offsetsMap.get(new TopicPartition(TOPIC, 0));
    //     assertThat(o.offset()).isEqualTo(5);

    //     assertThat(OffsetEncoder.decode(o.metadata())).isEmpty();
    // }

    static ConsumerRecord<?, ?> Record(int partition, String id) {
        String key = String.valueOf(id.charAt(1));
        long offset = Long.parseLong(String.valueOf(id.charAt(0)));
        String value = offset + key;
        return new ConsumerRecord<String, String>(TOPIC, partition, offset, key, value);
    }

    @Test
    void shouldReturnFullSequential() {
        assertThat(OffsetRepository.isFullySequential(Collections.emptySet())).isFalse();
        assertThat(OffsetRepository.isFullySequential(Set.of(0l))).isTrue();
        assertThat(OffsetRepository.isFullySequential(Set.of(1l, 9l, 8l))).isFalse();
        assertThat(OffsetRepository.isFullySequential(Set.of(1l, 2l, 3l))).isTrue();
        assertThat(OffsetRepository.isFullySequential(Set.of(4l, 5l, 6l))).isTrue();
        assertThat(OffsetRepository.isFullySequential(Set.of(4l, 5l, 6l, 8l))).isFalse();
    }

    @Test
    void shouldReturnOffsetMap4() {
        Map<TopicPartition, OffsetAndMetadata> offsetsMap = repo.getOffsets();
        repo.update(Record(0, "4A"));
        repo.update(Record(0, "5B"));
        repo.update(Record(0, "6C"));
        repo.update(Record(1, "7D"));
        OffsetAndMetadata o1 = offsetsMap.get(new TopicPartition(TOPIC, 1));
        assertThat(o1.offset()).isEqualTo(8);
        assertThat(o1.metadata()).isEmpty();

        repo.update(Record(1, "4E"));
        o1 = offsetsMap.get(new TopicPartition(TOPIC, 1));
        assertThat(o1.offset()).isEqualTo(5);
        assertThat(OffsetEncoder.decode(o1.metadata())).containsExactly(8l);

        repo.update(Record(1, "1E"));
        o1 = offsetsMap.get(new TopicPartition(TOPIC, 1));
        assertThat(o1.offset()).isEqualTo(2);
        assertThat(OffsetEncoder.decode(o1.metadata())).containsExactly(5l, 8l);

        repo.update(Record(1, "5E"));
        o1 = offsetsMap.get(new TopicPartition(TOPIC, 1));
        assertThat(o1.offset()).isEqualTo(6);
        assertThat(OffsetEncoder.decode(o1.metadata())).containsExactly(8l);

        repo.update(Record(1, "9F"));
        o1 = offsetsMap.get(new TopicPartition(TOPIC, 1));
        assertThat(o1.offset()).isEqualTo(5);
        assertThat(OffsetEncoder.decode(o1.metadata())).containsExactly(8l, 9l);

        repo.update(Record(2, "4F"));
    }
}
