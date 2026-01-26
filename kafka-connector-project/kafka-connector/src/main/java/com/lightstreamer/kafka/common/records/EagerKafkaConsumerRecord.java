
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
 * A {@link KafkaRecord} implementation that eagerly deserializes key and value.
 *
 * <p>Deserialization is performed immediately upon object creation. This approach allows for early
 * error detection but requires more upfront processing for all records, including those that may
 * not be fully consumed.
 *
 * @param <K> the type of the deserialized key
 * @param <V> the type of the deserialized value
 */
public final class EagerKafkaConsumerRecord<K, V> extends KafkaConsumerRecord<K, V> {

    private final K key;
    private final V value;

    /**
     * Constructs an {@link EagerKafkaConsumerRecord}, immediately deserializing the key and value.
     *
     * @param record the raw Kafka consumer record with byte array key and value
     * @param keyDeserializer the deserializer for the key
     * @param valueDeserializer the deserializer for the value
     */
    EagerKafkaConsumerRecord(
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        super(record);
        this.key = keyDeserializer.deserialize(record.topic(), record.key());
        this.value = valueDeserializer.deserialize(record.topic(), record.value());
    }

    @Override
    public K key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPayloadNull() {
        return value == null;
    }
}
