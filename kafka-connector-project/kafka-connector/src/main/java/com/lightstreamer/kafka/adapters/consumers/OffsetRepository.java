
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class OffsetRepository {

    record Event(String topic, int partition, long offset, boolean success) {
        public Event(String topic, int partition, long offset) {
            this(topic, partition, offset, true);
        }
    }

    static class OffsetEncoder {

        Set<Long> offsets = new ConcurrentSkipListSet<>();

        OffsetEncoder() {}

        OffsetEncoder(String str) {
            offsets.addAll(decode(str));
        }

        String encode() {
            return offsets.stream().map(String::valueOf).collect(Collectors.joining("."));
        }

        static String encode(List<Long> offsets) {
            return offsets.stream().map(String::valueOf).collect(Collectors.joining("."));
        }

        static Set<Long> decode(String str) {
            if (str.isEmpty()) {
                return Collections.emptySet();
            }
            return new TreeSet<>(
                    Arrays.asList(str.split("\\.")).stream().map(Long::valueOf).toList());
        }

        static String encodeAndAppend(String str, long offset) {
            ArrayList<Long> l = append(str, offset);
            return encode(l);
        }

        static ArrayList<Long> append(String str, long offset) {
            ArrayList<Long> l = new ArrayList<>(decode(str));
            l.add(offset);
            return l;
        }

        void encodeAndAppend(long offset) {
            offsets.add(offset);
        }
    }

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public void markSuccessRecord(ConsumerRecord<?, ?> record) {
        markEvent(new Event(record.topic(), record.partition(), record.offset(), true));
    }

    public void markFailureRecord(ConsumerRecord<?, ?> record) {
        markEvent(new Event(record.topic(), record.partition(), record.offset(), false));
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    void markEvent(Event e) {}

    static boolean isFullySequential(Collection<Long> offsets) {
        if (offsets.isEmpty()) {
            return false;
        }
        return offsets.size() == Collections.max(offsets) - Collections.min(offsets) + 1;
    }

    void update(ConsumerRecord<?, ?> record) {
        Event e = new Event(record.topic(), record.partition(), record.offset());
        TopicPartition tp = new TopicPartition(e.topic(), e.partition());
        offsets.compute(
                tp,
                (k, v) -> {
                    if (v == null) {
                        return new OffsetAndMetadata(e.offset() + 1, "");
                    }
                    if (e.offset() < v.offset()) {
                        ArrayList<Long> subsequent = OffsetEncoder.append(v.metadata(), v.offset());
                        if (isFullySequential(subsequent)
                                && Collections.min(subsequent) == e.offset()) {
                            return v;
                        }
                        return new OffsetAndMetadata(
                                e.offset() + 1, OffsetEncoder.encode(subsequent));
                    }
                    ArrayList<Long> subsequent = OffsetEncoder.append(v.metadata(), e.offset());
                    if (isFullySequential(subsequent)
                            && Collections.min(subsequent) == v.offset()) {
                        return new OffsetAndMetadata(e.offset() + 1, "");
                    }
                    return new OffsetAndMetadata(
                            v.offset(), OffsetEncoder.encodeAndAppend(v.metadata(), e.offset()));
                });
    }

    static Event PEvent(String topic, int partition, long offset, boolean success) {
        return new Event(topic, partition, offset, success);
    }

    static Event PEvent(String topic, int partition, long offset) {
        return new Event(topic, partition, offset, true);
    }

    static void testOffsetEncoder() {
        OffsetEncoder o = new OffsetEncoder();
        o.encodeAndAppend(1);
        o.encodeAndAppend(5);
        o.encodeAndAppend(3);

        String decoded = o.encode();
        OffsetEncoder o2 = new OffsetEncoder(decoded);
        System.out.println(o2.offsets);
    }

    public static void main(String[] args) {
        // testOffsetEncoder();
        testRepo();
    }

    public static void testRepo() {
        String TOPIC = "topic";
        OffsetRepository repo = new OffsetRepository();
        List<Event> events =
                List.of(
                        PEvent(TOPIC, 0, 1, true),
                        PEvent(TOPIC, 0, 2, true),
                        PEvent(TOPIC, 0, 3, true),
                        PEvent(TOPIC, 0, 5, true),
                        PEvent(TOPIC, 0, 7, true),
                        PEvent(TOPIC, 0, 4, false),
                        PEvent(TOPIC, 0, 6, false),
                        PEvent(TOPIC, 0, 8, false));

        events.stream().forEach(repo::markEvent);
    }
}
