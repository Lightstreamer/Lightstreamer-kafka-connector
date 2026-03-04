
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

import static org.apache.kafka.common.serialization.Serdes.String;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Records {

    public static KafkaRecord<GenericRecord, ?> KafkaRecordFromKey(GenericRecord key) {
        return KafkaRecord(key, null);
    }

    public static KafkaRecord<?, GenericRecord> KafkaRecordFromValue(GenericRecord value) {
        return KafkaRecord(null, value);
    }

    public static KafkaRecord<?, DynamicMessage> KafkaRecordFromValue(DynamicMessage value) {
        return KafkaRecord(null, value);
    }

    public static KafkaRecord<DynamicMessage, ?> KafkaRecordFromKey(DynamicMessage key) {
        return KafkaRecord(key, null);
    }

    public static KafkaRecord<JsonNode, ?> KafkaRecordFromKey(JsonNode key) {
        return KafkaRecord(key, null);
    }

    public static KafkaRecord<?, JsonNode> KafkaRecordFromValue(JsonNode value) {
        return KafkaRecord(null, value);
    }

    public static KafkaRecord<String, ?> KafkaRecordFromKey(String key) {
        return KafkaRecord(key, null);
    }

    public static KafkaRecord<?, String> KafkaRecordFromValue(String value) {
        return KafkaRecord(null, value);
    }

    public static KafkaRecord<Object, ?> KafkaRecordFromKey(Object key) {
        return KafkaRecord(key, null);
    }

    public static KafkaRecord<?, Object> KafkaRecordFromValue(Object value) {
        return KafkaRecord(null, value);
    }

    public static KafkaRecord<?, Integer> KafkaRecordFromIntValue(int value) {
        return KafkaRecord(null, value);
    }

    public static <K, V> KafkaRecord<K, V> KafkaRecord(K key, V value) {
        return KafkaRecordWithHeaders(key, value, new RecordHeaders());
    }

    public static <K, V> KafkaRecord<K, V> KafkaRecord(String topic, K key, V value) {
        return KafkaRecordWithHeaders(topic, key, value, new RecordHeaders());
    }

    public static <K, V> KafkaRecord<K, V> KafkaRecordWithHeaders(K key, V value, Headers headers) {
        return KafkaRecordWithHeaders("record-topic", key, value, headers);
    }

    public static <K, V> KafkaRecord<K, V> KafkaRecordWithHeaders(
            String topic, K key, V value, Headers headers) {
        return KafkaRecord.from(topic, 150, 120, -1, key, value, headers);
    }

    public static KafkaRecord<String, String> StringKafkaRecord(
            String topic, String key, String value) {
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        topic,
                        150,
                        120,
                        String().serializer().serialize(topic, key),
                        String().serializer().serialize(topic, value));
        return KafkaRecord.fromDeferred(
                record,
                new DeserializerPair<>(String().deserializer(), String().deserializer()),
                null);
    }

    public static KafkaRecord<String, String> KafkaRecord(String topic, int partition, String id) {
        return KafkaRecord.fromDeferred(
                ConsumerRecord(topic, partition, id),
                new DeserializerPair<>(String().deserializer(), String().deserializer()),
                null);
    }

    public static ConsumerRecord<byte[], byte[]> ConsumerRecord(
            String topic, int partition, String id) {
        String[] tokens = id.split("-");
        String key = tokens[0];
        long offset = Long.parseLong(tokens[1]);
        String value = offset + key;
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                String().serializer().serialize(topic, key),
                String().serializer().serialize(topic, value));
    }

    public static KafkaRecord<Object, Object> sinkFromValue(
            String topic, Schema valueSchema, Object value) {
        return sink(topic, null, null, valueSchema, value);
    }

    public static KafkaRecord<Object, Object> sinkFromKey(
            String topic, Schema keySchema, Object key) {
        return sink(topic, keySchema, key, null, null);
    }

    public static KafkaRecord<Object, Object> sinkWithHeaders(
            Object key, Object value, org.apache.kafka.connect.header.Headers headers) {
        return KafkaRecord.from(
                new SinkRecord(
                        "record-topic",
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

    public static ConsumerRecords<byte[], byte[]> generateRecords(
            String topic, int numOfRecords, List<String> keys) {
        return generateRecords(topic, numOfRecords, keys, 1);
    }

    public static ConsumerRecords<byte[], byte[]> generateRecords(
            String topic, int numOfRecords, List<String> keys, int partitions) {

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        Map<String, Integer> eventCounter = new HashMap<>();
        Map<Integer, Integer> offsetCounter = new HashMap<>();
        SecureRandom secureRandom = new SecureRandom();

        Serializer<String> keySerializer = String().serializer();
        Serializer<String> valueSerializer = String().serializer();

        // Generate the records list
        for (int i = 0; i < numOfRecords; i++) {
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
            records.add(
                    new ConsumerRecord<>(
                            topic,
                            partition,
                            offset,
                            keySerializer.serialize(topic, recordKey),
                            valueSerializer.serialize(topic, recordValue)));
        }

        // Group records by partition to be passed to the ConsumerRecords instance
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionsToRecords =
                records.stream()
                        .collect(
                                groupingBy(
                                        record -> new TopicPartition(topic, record.partition()),
                                        mapping(Function.identity(), toList())));
        return new ConsumerRecords<>(partitionsToRecords);
    }
}
