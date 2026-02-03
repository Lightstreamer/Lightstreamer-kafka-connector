
/*
 * Copyright (C) 2025 Lightstreamer Srl
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
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A {@link KafkaRecord} implementation that lazily deserializes key and value.
 *
 * <p>Deserialization is deferred until the {@link #key()} or {@link #value()} methods are called,
 * and results are cached for subsequent accesses. This approach is useful when not all records will
 * be fully consumed, or when deserialization is expensive.
 *
 * <p>Thread safety: The deserialization result caching uses {@code volatile} fields to ensure
 * visibility across threads. However, synchronization is not required as records are guaranteed to
 * not be consumed concurrently. The {@code volatile} semantics ensure that if a record is accessed
 * by different threads (non-concurrently), the deserialized values are properly propagated.
 *
 * @param <K> the type of the deserialized key
 * @param <V> the type of the deserialized value
 * @see #key()
 * @see #value()
 */
public final class DeferredKafkaConsumerRecord<K, V> extends KafkaConsumerRecord<K, V> {

    private static final Object UNINITIALIZED = new Object();

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final byte[] rawKey;
    private final byte[] rawValue;
    private V cachedValue = (V) UNINITIALIZED;
    private K cachedKey = (K) UNINITIALIZED;

    /**
     * Constructs a {@link DeferredKafkaConsumerRecord}.
     *
     * @param record the raw Kafka consumer record with byte array key and value
     * @param deserializerPair the pair of deserializers for key and value
     * @param batch the parent batch this record belongs to, or {@code null} if not associated
     */
    DeferredKafkaConsumerRecord(
            ConsumerRecord<byte[], byte[]> record,
            DeserializerPair<K, V> deserializerPair,
            RecordBatch<K, V> batch) {
        super(record, batch);
        this.rawKey = record.key();
        this.rawValue = record.value();
        this.keyDeserializer = deserializerPair.keyDeserializer();
        this.valueDeserializer = deserializerPair.valueDeserializer();
    }

    /**
     * Returns the deserialized key, deserializing lazily on first access.
     *
     * <p>The deserialized key is cached after the first call. No synchronization is required as
     * records are not consumed concurrently.
     *
     * @return the deserialized key, or {@code null} if the raw key is null
     */
    @Override
    public K key() {
        K key = cachedKey;
        if (key == UNINITIALIZED) {
            key = keyDeserializer.deserialize(record.topic(), rawKey);
            cachedKey = key;
        }
        return key;
    }

    /**
     * Returns the deserialized value, deserializing lazily on first access.
     *
     * <p>The deserialized value is cached after the first call. No synchronization is required as
     * records are not consumed concurrently.
     *
     * @return the deserialized value, or {@code null} if the raw value is null
     */
    @Override
    public V value() {
        V value = cachedValue;
        if (value == UNINITIALIZED) {
            value = valueDeserializer.deserialize(record.topic(), rawValue);
            cachedValue = value;
        }
        return value;
    }

    /**
     * Checks whether the record payload (value) is null.
     *
     * @return {@code true} if the raw value is null, {@code false} otherwise
     */
    @Override
    public boolean isPayloadNull() {
        return rawValue == null;
    }
}
