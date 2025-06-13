
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

package com.lightstreamer.kafka.test_utils;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class Records {

    public static KafkaRecord<GenericRecord, ?> fromKey(GenericRecord key) {
        return record(key, null);
    }

    public static KafkaRecord<?, GenericRecord> fromValue(GenericRecord value) {
        return record(null, value);
    }

    public static KafkaRecord<?, DynamicMessage> fromValue(DynamicMessage value) {
        return record(null, value);
    }

    public static KafkaRecord<DynamicMessage, ?> fromKey(DynamicMessage key) {
        return record(key, null);
    }

    public static KafkaRecord<JsonNode, ?> fromKey(JsonNode key) {
        return record(key, null);
    }

    public static KafkaRecord<?, JsonNode> fromValue(JsonNode value) {
        return record(null, value);
    }

    public static KafkaRecord<String, ?> fromKey(String key) {
        return record(key, null);
    }

    public static KafkaRecord<?, String> fromValue(String value) {
        return record(null, value);
    }

    public static KafkaRecord<Object, ?> fromKey(Object key) {
        return record(key, null);
    }

    public static KafkaRecord<?, Object> fromValue(Object value) {
        return record(null, value);
    }

    public static KafkaRecord<?, Integer> fromIntValue(int value) {
        return record(null, value);
    }

    public static <K, V> KafkaRecord<K, V> record(K key, V value) {
        return recordWithHeaders(key, value, new RecordHeaders());
    }

    public static <K, V> KafkaRecord<K, V> recordWithHeaders(K key, V value, Headers headers) {
        return recordWithHeaders("record-topic", key, value, headers);
    }

    public static <K, V> KafkaRecord<K, V> recordWithHeaders(
            String topic, K key, V value, Headers headers) {
        return KafkaRecord.from(
                new ConsumerRecord<>(
                        topic,
                        150,
                        120,
                        ConsumerRecord.NO_TIMESTAMP,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        ConsumerRecord.NULL_SIZE,
                        ConsumerRecord.NULL_SIZE,
                        key,
                        value,
                        headers,
                        Optional.empty()));
    }

    public static <K, V> KafkaRecord<K, V> record(String topic, K key, V value) {
        return KafkaRecord.from(ConsumerRecord(topic, key, value));
    }

    public static <K, V> ConsumerRecord<K, V> ConsumerRecord(String topic, K key, V value) {
        return new ConsumerRecord<>(
                topic,
                150,
                120,
                ConsumerRecord.NO_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                key,
                value,
                new RecordHeaders(),
                Optional.empty());
    }

    public static ConsumerRecord<String, String> ConsumerRecord(
            String topic, int partition, String id) {
        String[] tokens = id.split("-");
        String key = tokens[0];
        long offset = Long.parseLong(tokens[1]);
        String value = offset + key;
        return new ConsumerRecord<>(topic, partition, offset, key, value);
    }

    public static KafkaRecord<Object, Object> sinkFromValue(
            String topic, Schema valueSchema, Object value) {
        return sink(topic, null, null, valueSchema, value);
    }

    public static KafkaRecord<Object, Object> sinkFromKey(
            String topic, Schema keySchema, Object key) {
        return sink(topic, keySchema, key, null, null);
    }

    public static KafkaRecord<Object, Object> sinkFromHeaders(
            String topic, org.apache.kafka.connect.header.Headers headers) {
        return KafkaRecord.from(
                new SinkRecord(
                        topic,
                        150,
                        null,
                        null,
                        null,
                        null,
                        120,
                        (long) -1,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        headers));
    }

    public static KafkaRecord<Object, Object> sinkRecord(String topic, Object key, Object value) {
        return KafkaRecord.from(
                new SinkRecord(
                        topic,
                        150,
                        null,
                        key,
                        null,
                        value,
                        120,
                        (long) -1,
                        TimestampType.NO_TIMESTAMP_TYPE));
    }

    public static KafkaRecord<Object, Object> sink(
            String topic, Schema keySchema, Object key, Schema valueSchema, Object value) {
        return KafkaRecord.from(
                new SinkRecord(
                        topic,
                        150,
                        keySchema,
                        key,
                        valueSchema,
                        value,
                        120,
                        (long) -1,
                        TimestampType.NO_TIMESTAMP_TYPE));
    }

    public static ConsumerRecords<String, String> generateRecords(
            String topic, int size, List<String> keys) {
        return generateRecords(topic, size, keys, 1);
    }

    public static ConsumerRecords<String, String> generateRecords(
            String topic, int size, List<String> keys, int partitions) {

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        Map<String, Integer> eventCounter = new HashMap<>();
        Map<Integer, Integer> offsetCounter = new HashMap<>();
        SecureRandom secureRandom = new SecureRandom();

        // Generate the records list
        for (int i = 0; i < size; i++) {
            String recordKey = null;
            String recordValue;
            String eventCounterKey = "noKey";
            int partition;

            if (keys.size() > 0) {
                // Select randomly one of the passed keys
                int index = secureRandom.nextInt(keys.size());
                recordKey = keys.get(index);
                eventCounterKey = recordKey;
                // Generate a value by adding a counter suffix to the key: key "a" -> value
                // "a-4"
                int counter = eventCounter.compute(eventCounterKey, (k, v) -> v == null ? 1 : ++v);
                recordValue = "%s-%d".formatted(recordKey, counter);
                // Select a partition based on the key hash code
                partition = recordKey.hashCode() % partitions;
            } else {
                // Generate a value simply by adding a counter suffix.
                // Note that in this case the counter is global
                int counter = eventCounter.compute(eventCounterKey, (k, v) -> v == null ? 1 : ++v);
                recordValue = "%s-%d".formatted("EVENT", counter);
                // Round robin selection of the partition
                partition = i % partitions;
            }

            // Increment the offset relative to the selected partition
            int offset = offsetCounter.compute(partition, (p, o) -> o == null ? 0 : ++o);
            records.add(new ConsumerRecord<>(topic, partition, offset, recordKey, recordValue));
        }

        // Group records by partition to be passed to the ConsumerRecords instance
        Map<TopicPartition, List<ConsumerRecord<String, String>>> partitionsToRecords =
                records.stream()
                        .collect(
                                groupingBy(
                                        record -> new TopicPartition(topic, record.partition()),
                                        mapping(Function.identity(), toList())));
        return new ConsumerRecords<>(partitionsToRecords);
    }
}
