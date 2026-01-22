
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

public final class DeferredKafkaConsumerRecord<K, V> implements KafkaRecord<K, V> {

    private final ConsumerRecord<byte[], byte[]> record;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final byte[] rawKey;
    private final byte[] rawValue;
    private volatile boolean isValueCached = false;
    private volatile boolean isKeyCached = false;
    private volatile V cachedValue = null;
    private volatile K cachedKey = null;

    DeferredKafkaConsumerRecord(
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        this.record = record;
        this.rawKey = record.key();
        this.rawValue = record.value();
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public K key() {
        if (!isKeyCached) {
            cachedKey = keyDeserializer.deserialize(record.topic(), rawKey);
            isKeyCached = true;
        }
        return cachedKey;
    }

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
        return rawValue == null || rawValue.length == 0;
    }

    @Override
    public long timestamp() {
        return record.timestamp();
    }

    @Override
    public long offset() {
        return record.offset();
    }

    @Override
    public String topic() {
        return record.topic();
    }

    @Override
    public int partition() {
        return record.partition();
    }

    @Override
    public KafkaHeaders headers() {
        return KafkaHeaders.from(record.headers());
    }
}
