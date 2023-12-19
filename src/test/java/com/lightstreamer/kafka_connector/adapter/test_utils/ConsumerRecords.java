package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.util.Optional;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import com.fasterxml.jackson.databind.JsonNode;

public class ConsumerRecords {

    public static ConsumerRecord<GenericRecord, ?> recordWithKey(GenericRecord key) {
        return record(key, null);
    }

    public static ConsumerRecord<?, GenericRecord> recordWithValue(GenericRecord value) {
        return record(null, value);
    }

    public static ConsumerRecord<JsonNode, ?> recordWithKey(JsonNode key) {
        return record(key, null);
    }

    public static ConsumerRecord<?, JsonNode> recordWithValue(JsonNode value) {
        return record(null, value);
    }

    public static ConsumerRecord<GenericRecord, GenericRecord> recordWithGenericRecordPair(String topic,
            GenericRecord key, GenericRecord value) {
        return record(topic, key, value);
    }

    public static ConsumerRecord<String, ?> recordWithKey(String key) {
        return record(key, null);
    }

    public static ConsumerRecord<?, String> recordWithStringValue(String value) {
        return record(null, value);
    }

    public static <K, V> ConsumerRecord<K, V> record(K key, V value) {
        return new ConsumerRecord<K, V>(
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
                Optional.empty());
    }

    public static <K, V> ConsumerRecord<K, V> record(String topic, K key, V value) {
        return new ConsumerRecord<K, V>(
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
}
