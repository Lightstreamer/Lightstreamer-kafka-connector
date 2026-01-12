
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

package com.lightstreamer.kafka.adapters;

import org.apache.kafka.common.header.Headers;

/**
 * Represents a raw Kafka record containing all the essential metadata and payload information from
 * a consumed Kafka message.
 *
 * <p>This record encapsulates the complete structure of a Kafka message including:
 *
 * <ul>
 *   <li>Topic and partition information for message location
 *   <li>Offset for message ordering and position tracking
 *   <li>Timestamp for message timing
 *   <li>Raw key and value as byte arrays for flexible data handling
 *   <li>Headers for additional metadata
 * </ul>
 *
 * <p>The raw byte array format for key and value allows handling of any serialization format
 * without imposing specific deserialization requirements at this level.
 *
 * @param topic the Kafka topic name from which this record was consumed
 * @param partition the partition number within the topic
 * @param offset the offset of this record within the partition
 * @param timestamp the timestamp associated with this record
 * @param key the raw key bytes of the Kafka message, may be null
 * @param value the raw value bytes of the Kafka message, may be null
 * @param headers the collection of headers associated with this Kafka message
 */
public record RawKafkaRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        byte[] key,
        byte[] value,
        Headers headers) {}
