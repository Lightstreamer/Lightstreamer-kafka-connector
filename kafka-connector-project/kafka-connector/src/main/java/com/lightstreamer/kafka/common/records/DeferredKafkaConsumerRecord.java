
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

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final byte[] rawKey;
    private final byte[] rawValue;
    private volatile boolean isValueCached = false;
    private volatile boolean isKeyCached = false;
    private volatile V cachedValue = null;
    private volatile K cachedKey = null;

    /**
     * Constructs a {@link DeferredKafkaConsumerRecord}.
     *
     * @param record the raw Kafka consumer record with byte array key and value
     * @param keyDeserializer the deserializer for the key
     * @param valueDeserializer the deserializer for the value
     */
    DeferredKafkaConsumerRecord(
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        super(record);
        this.rawKey = record.key();
        this.rawValue = record.value();
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
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
        if (!isKeyCached) {
            cachedKey = keyDeserializer.deserialize(record.topic(), rawKey);
            isKeyCached = true;
        }
        return cachedKey;
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
        if (!isValueCached) {
            cachedValue = valueDeserializer.deserialize(record.topic(), rawValue);
            isValueCached = true;
        }
        return cachedValue;
    }

    @Override
    public boolean isPayloadNull() {
        return rawValue == null;
    }
}
