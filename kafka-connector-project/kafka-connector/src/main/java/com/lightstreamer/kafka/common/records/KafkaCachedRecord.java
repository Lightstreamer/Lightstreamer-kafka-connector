
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

import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;

import org.apache.kafka.common.header.Headers;

final class KafkaCachedRecord<K, V> implements KafkaRecord<K, V> {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final Deferred<K> key;
    private final Deferred<V> value;
    private final Headers headers;

    KafkaCachedRecord(
            String topic,
            int partition,
            long offset,
            long timestamp,
            Deferred<K> key,
            Deferred<V> value,
            Headers headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    @Override
    public K key() {
        return key.load();
    }

    @Override
    public V value() {
        return value.load();
    }

    @Override
    public boolean isPayloadNull() {
        return value.isNull();
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public KafkaHeaders headers() {
        return new KafkaHeadersImpl(headers);
    }

    @Override
    public RecordBatch<K, V> getBatch() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBatch'");
    }
}
