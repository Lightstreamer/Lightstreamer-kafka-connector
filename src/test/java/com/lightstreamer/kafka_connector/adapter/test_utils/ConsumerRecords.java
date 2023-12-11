package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.util.Optional;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

public class ConsumerRecords {

    public static ConsumerRecord<GenericRecord, ?> recordWithGenericRecordKey(GenericRecord key) {
        return record(key, null);
    }

    public static ConsumerRecord<?, GenericRecord> recordWithGenericRecordValue(GenericRecord value) {
        return record(null, value);
    }

    public static ConsumerRecord<GenericRecord, GenericRecord> recordWithGenericRecordPair(GenericRecord key, GenericRecord value) {
        return record(key, value);
    }

    public static <K, V> ConsumerRecord<K, V> record(K key, V value) {
        return new ConsumerRecord<K,V>(
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
        return new ConsumerRecord<K,V>(
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
