
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

/**
 * A simple Java record implementation of {@link KafkaRecord} for testing purposes.
 *
 * <p>This record encapsulates all components of a Kafka record (topic, partition, offset,
 * timestamp, key, value, and headers). It is primarily used in unit tests to create {@link
 * KafkaRecord} instances with arbitrary values.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @param topic the topic name
 * @param partition the partition number
 * @param offset the record offset
 * @param timestamp the record timestamp
 * @param key the record key
 * @param value the record value
 * @param headers the record headers
 */
record SimpleKafkaRecord<K, V>(
        String topic,
        int partition,
        long offset,
        long timestamp,
        K key,
        V value,
        KafkaHeaders headers)
        implements KafkaRecord<K, V> {

    /**
     * Checks whether the record payload (value) is null.
     *
     * @return {@code true} if the value is null, {@code false} otherwise
     */
    @Override
    public boolean isPayloadNull() {
        return value == null;
    }
}
