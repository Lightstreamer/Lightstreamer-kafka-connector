
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

package com.lightstreamer.kafka.common.records;

import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public interface KafkaRecord<K, V> {

    public interface KafkaHeader {

        String key();

        byte[] value();

        default int localIndex() {
            return -1;
        }
    }

    public interface KafkaHeaders extends Iterable<KafkaHeader> {

        boolean has(String key);

        KafkaHeader get(int index);

        List<KafkaHeader> headers(String key);

        int size();

        static KafkaHeaders from(org.apache.kafka.connect.header.Headers headers) {
            return new KafkaHeadersImpl(headers);
        }

        static KafkaHeaders from(org.apache.kafka.common.header.Headers headers) {
            return new KafkaHeadersImpl(headers);
        }
    }

    public static <K, V> KafkaRecord<K, V> fromDeferred(
            ConsumerRecord<Deferred<K>, Deferred<V>> record) {
        return new DeferredKafkaConsumerRecord<>(record);
    }

    public static <K, V> KafkaRecord<K, V> from(ConsumerRecord<K, V> record) {
        return new KafkaConsumerRecord<>(record);
    }

    public static KafkaRecord<Object, Object> from(SinkRecord record) {
        return new KafkaSinkRecord(record);
    }

    static <K, V> List<KafkaRecord<K, V>> listFrom(ConsumerRecords<K, V> consumerRecords) {
        List<KafkaRecord<K, V>> kafkaRecords = new ArrayList<>(consumerRecords.count());
        consumerRecords
                .partitions()
                .forEach(
                        partition -> {
                            List<ConsumerRecord<K, V>> recordsByPartition =
                                    consumerRecords.records(partition);
                            for (int i = 0; i < recordsByPartition.size(); i++) {
                                ConsumerRecord<K, V> record = recordsByPartition.get(i);
                                kafkaRecords.add(KafkaRecord.from(record));
                            }
                        });
        return kafkaRecords;
    }

    static <K, V> List<KafkaRecord<K, V>> listFromDeferred(
            ConsumerRecords<Deferred<K>, Deferred<V>> consumerRecords) {
        return consumerRecords.partitions().parallelStream()
                .flatMap(partition -> consumerRecords.records(partition).stream())
                .map(KafkaRecord::fromDeferred)
                .collect(Collectors.toList());
    }

    static <K, V> List<KafkaRecord<K, V>> listFromDeferred2(
            ConsumerRecords<Deferred<K>, Deferred<V>> consumerRecords) {
        List<KafkaRecord<K, V>> kafkaRecords = new ArrayList<>(consumerRecords.count());
        consumerRecords
                .partitions()
                .forEach(
                        partition -> {
                            List<ConsumerRecord<Deferred<K>, Deferred<V>>> recordsByPartition =
                                    consumerRecords.records(partition);
                            for (int i = 0, n = recordsByPartition.size(); i < n; i++) {
                                ConsumerRecord<Deferred<K>, Deferred<V>> record =
                                        recordsByPartition.get(i);
                                kafkaRecords.add(KafkaRecord.fromDeferred(record));
                            }
                        });
        return kafkaRecords;
    }

    K key();

    V value();

    boolean isPayloadNull();

    long timestamp();

    long offset();

    String topic();

    int partition();

    KafkaHeaders headers();
}
