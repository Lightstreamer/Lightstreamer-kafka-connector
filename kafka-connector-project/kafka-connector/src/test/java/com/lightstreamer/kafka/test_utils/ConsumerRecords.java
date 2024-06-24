
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Optional;

public class ConsumerRecords {

    public static KafkaRecord<GenericRecord, ?> fromKey(GenericRecord key) {
        return record(key, null);
    }

    public static KafkaRecord<?, GenericRecord> fromValue(GenericRecord value) {
        return record(null, value);
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
        return KafkaRecord.from(
                new ConsumerRecord<>(
                        "record-topic",
                        150,
                        120,
                        ConsumerRecord.NO_TIMESTAMP,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        ConsumerRecord.NULL_SIZE,
                        ConsumerRecord.NULL_SIZE,
                        key,
                        value,
                        new RecordHeaders(),
                        Optional.empty()));
    }

    public static <K, V> KafkaRecord<K, V> record(String topic, K key, V value) {
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
                        new RecordHeaders(),
                        Optional.empty()));
    }

    public static KafkaRecord<Object, Object> sinkFromValue(
            String topic, Schema valueSchema, Object value) {
        return sink(topic, null, null, valueSchema, value);
    }

    public static KafkaRecord<Object, Object> sinkFromKey(
            String topic, Schema keySchema, Object key) {
        return sink(topic, keySchema, key, null, null);
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
}
