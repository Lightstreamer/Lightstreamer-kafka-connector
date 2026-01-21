
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

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
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return new DeferredKafkaConsumerRecord<>(record, keyDeserializer, valueDeserializer);
    }

    public static <K, V> KafkaRecord<K, V> fromEager(
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return new EagerKafkaConsumerRecord<>(record, keyDeserializer, valueDeserializer);
    }

    // Only for testing purposes
    public static <K, V> KafkaRecord<K, V> from(
            String topic,
            int partition,
            long offset,
            long timestamp,
            K key,
            V value,
            Headers headers) {
        KafkaHeaders kafkaHeaders = headers != null ? KafkaHeaders.from(headers) : null;
        return new SimpleKafkaRecord<>(
                topic, partition, offset, timestamp, key, value, kafkaHeaders);
    }

    public static <K, V> KafkaRecord<K, V> from(ConsumerRecord<K, V> record) {
        return new KafkaConsumerRecord<>(record);
    }

    public static KafkaRecord<Object, Object> from(SinkRecord record) {
        return new KafkaSinkRecord(record);
    }

    static <K, V> List<KafkaRecord<K, V>> listFromEager(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        List<KafkaRecord<K, V>> kafkaRecords = new ArrayList<>(consumerRecords.count());
        consumerRecords
                .partitions()
                .forEach(
                        partition -> {
                            List<ConsumerRecord<byte[], byte[]>> records =
                                    consumerRecords.records(partition);
                            for (int i = 0, n = records.size(); i < n; i++) {
                                kafkaRecords.add(
                                        fromEager(
                                                records.get(i),
                                                keyDeserializer,
                                                valueDeserializer));
                            }
                        });
        return kafkaRecords;
    }

    static <K, V> List<KafkaRecord<K, V>> listFromDeferred(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        List<KafkaRecord<K, V>> kafkaRecords = new ArrayList<>(consumerRecords.count());
        consumerRecords
                .partitions()
                .forEach(
                        partition -> {
                            List<ConsumerRecord<byte[], byte[]>> records =
                                    consumerRecords.records(partition);
                            for (int i = 0, n = records.size(); i < n; i++) {
                                kafkaRecords.add(
                                        fromDeferred(
                                                records.get(i),
                                                keyDeserializer,
                                                valueDeserializer));
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
