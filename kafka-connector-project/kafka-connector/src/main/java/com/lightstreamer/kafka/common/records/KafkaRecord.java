
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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

/**
 * A generic interface representing a Kafka record with typed key and value.
 *
 * <p>This interface serves as an abstraction layer over Kafka records from various sources (Kafka
 * Consumer API, Kafka Connect Sink API) and provides factory methods to create implementations with
 * different deserialization strategies.
 *
 * <p>Implementations may deserialize key and value eagerly or defer deserialization until access
 * time, allowing for flexible performance tuning.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see #fromDeferred(ConsumerRecord, DeserializerPair, RecordBatch)
 * @see #fromEager(ConsumerRecord, DeserializerPair, RecordBatch)
 */
public interface KafkaRecord<K, V> {

    /**
     * Represents a single Kafka message header with key-value pair.
     *
     * <p>Headers are optional metadata associated with Kafka messages. The {@link #localIndex()}
     * method provides the index of this header within headers with the same key.
     */
    public interface KafkaHeader {
        /**
         * Returns the header key.
         *
         * @return the header key as a string
         */
        String key();

        /**
         * Returns the header value.
         *
         * @return the header value as a byte array
         */
        byte[] value();

        /**
         * Returns the local index of this header within headers with the same key.
         *
         * <p>Returns {@code -1} if this is the only header with this key, or the 0-based index if
         * there are multiple headers with the same key.
         *
         * @return the local index, or {@code -1} if no indexing is needed
         */
        default int localIndex() {
            return -1;
        }
    }

    /**
     * A collection of Kafka message headers.
     *
     * <p>Provides efficient access to headers by key or by index, and supports iteration over all
     * headers. Headers are typically associated with a single Kafka record.
     */
    public interface KafkaHeaders extends Iterable<KafkaHeader> {
        /**
         * Checks whether a header with the given key exists.
         *
         * @param key the header key to check
         * @return {@code true} if a header with the given key exists, {@code false} otherwise
         */
        boolean has(String key);

        /**
         * Returns the header at the given index.
         *
         * @param index the index of the header
         * @return the header at the given index
         * @throws IndexOutOfBoundsException if index is out of range
         */
        KafkaHeader get(int index);

        /**
         * Returns all headers with the given key.
         *
         * @param key the header key
         * @return a list of headers with the given key, or {@code null} if no headers with this key
         *     exist
         */
        List<KafkaHeader> headers(String key);

        /**
         * Returns the total number of headers.
         *
         * @return the number of headers
         */
        int size();

        /**
         * Creates a {@code KafkaHeaders} instance from Kafka Connect headers.
         *
         * @param headers the Kafka Connect headers
         * @return a new {@code KafkaHeaders} instance wrapping the given headers
         */
        static KafkaHeaders from(org.apache.kafka.connect.header.Headers headers) {
            return new KafkaHeadersImpl(headers);
        }

        /**
         * Creates a {@code KafkaHeaders} instance from Kafka Consumer API headers.
         *
         * @param headers the Kafka Consumer API headers
         * @return a new {@code KafkaHeaders} instance wrapping the given headers
         */
        static KafkaHeaders from(org.apache.kafka.common.header.Headers headers) {
            return new KafkaHeadersImpl(headers);
        }
    }

    /**
     * A pair of deserializers for Kafka record keys and values.
     *
     * <p>This record encapsulates a key deserializer and a value deserializer, used for converting
     * byte arrays to typed objects during record deserialization.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     */
    public record DeserializerPair<K, V>(
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {}

    /**
     * Creates a {@link KafkaRecord} with deferred deserialization and batch association.
     *
     * <p>Deserialization is performed lazily when {@link #key()} or {@link #value()} methods are
     * called, and results are cached for subsequent accesses. This variant allows associating the
     * record with its parent batch for tracking processing completion.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param record the raw Kafka consumer record with byte array key and value
     * @param deserializerPair the pair of deserializers for key and value
     * @param batch the parent batch this record belongs to, or {@code null} if not associated
     * @return a new {@link KafkaRecord} with deferred deserialization
     * @see DeferredKafkaConsumerRecord
     */
    public static <K, V> KafkaRecord<K, V> fromDeferred(
            ConsumerRecord<byte[], byte[]> record,
            DeserializerPair<K, V> deserializerPair,
            RecordBatch<K, V> batch) {
        return new DeferredKafkaConsumerRecord<>(record, deserializerPair, batch);
    }

    /**
     * Creates a {@link KafkaRecord} with eager deserialization and batch association.
     *
     * <p>Deserialization is performed immediately upon object creation. This variant allows
     * associating the record with its parent batch for tracking processing completion.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param record the raw Kafka consumer record with byte array key and value
     * @param deserializerPair the pair of deserializers for key and value
     * @param batch the parent batch this record belongs to, or {@code null} if not associated
     * @return a new {@link KafkaRecord} with eager deserialization
     * @see EagerKafkaConsumerRecord
     */
    public static <K, V> KafkaRecord<K, V> fromEager(
            ConsumerRecord<byte[], byte[]> record,
            DeserializerPair<K, V> deserializerPair,
            RecordBatch<K, V> batch) {
        return new EagerKafkaConsumerRecord<>(record, deserializerPair, batch);
    }

    /**
     * Creates a {@link KafkaRecord} from individual components.
     *
     * <p>This factory method is primarily intended for testing purposes and allows creating a
     * record from individual topic, partition, offset, timestamp, key, value, and headers.
     *
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @param topic the topic name
     * @param partition the partition number
     * @param offset the record offset
     * @param timestamp the record timestamp
     * @param key the record key
     * @param value the record value
     * @param headers the record headers, or {@code null} if no headers
     * @return a new {@link KafkaRecord} with the specified components
     * @see SimpleKafkaRecord
     */
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

    /**
     * Creates a {@link KafkaRecord} from a Kafka Connect {@link SinkRecord}.
     *
     * <p>The created record wraps the {@code SinkRecord} and provides access to its properties
     * through the {@link KafkaRecord} interface.
     *
     * @param record the Kafka Connect {@link SinkRecord} to wrap
     * @return a new {@link KafkaRecord} wrapping the given {@code SinkRecord}
     * @see KafkaSinkRecord
     */
    public static KafkaRecord<Object, Object> from(SinkRecord record) {
        return new KafkaSinkRecord(record);
    }

    /**
     * Returns the deserialized key of this record.
     *
     * @return the record key, or {@code null} if the key is null
     */
    K key();

    /**
     * Returns the deserialized value of this record.
     *
     * @return the record value, or {@code null} if the value is null
     */
    V value();

    /**
     * Checks whether the record payload (value) is null.
     *
     * @return {@code true} if the record value is null, {@code false} otherwise
     */
    boolean isPayloadNull();

    /**
     * Returns the timestamp of this record.
     *
     * <p>The timestamp represents when the record was produced (producer-side) or when it was
     * appended to the broker (broker-side), depending on the timestamp type configuration.
     *
     * @return the record timestamp in milliseconds since epoch
     */
    long timestamp();

    /**
     * Returns the offset of this record in its partition.
     *
     * @return the record offset
     */
    long offset();

    /**
     * Returns the topic name where this record was produced.
     *
     * @return the topic name
     */
    String topic();

    /**
     * Returns the partition number where this record was produced.
     *
     * @return the partition number
     */
    int partition();

    /**
     * Returns the headers associated with this record.
     *
     * @return the record headers
     */
    KafkaHeaders headers();

    /**
     * Returns the parent batch this record belongs to.
     *
     * @return the parent {@link RecordBatch}, or {@code null} if not associated with a batch
     */
    RecordBatch<K, V> getBatch();
}
