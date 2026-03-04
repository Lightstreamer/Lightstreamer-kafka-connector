
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

import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeader;

/**
 * A record implementation of {@link KafkaHeader} that represents a Kafka message header.
 *
 * <p>This record encapsulates a header's key, value, and its local index within a collection of
 * headers. It provides constructors to create instances from both Kafka Consumer API headers
 * ({@link org.apache.kafka.common.header.Header}) and Kafka Connect API headers ({@link
 * org.apache.kafka.connect.header.Header}).
 *
 * @param key the header key
 * @param value the header value as a byte array
 * @param localIndex the index of this header within the local header collection
 */
final record KafkaRecordHeader(String key, byte[] value, int localIndex) implements KafkaHeader {

    KafkaRecordHeader(org.apache.kafka.common.header.Header header, int localIndex) {
        this(header.key(), header.value(), localIndex);
    }

    KafkaRecordHeader(org.apache.kafka.connect.header.Header header, int localIndex) {
        this(header.key(), checkValue(header.value()), localIndex);
    }

    /**
     * Checks and extracts the byte array value from the given object.
     *
     * @param value the object to check and extract bytes from
     * @return the byte array if the value is an instance of byte[], otherwise an empty byte array
     */
    private static byte[] checkValue(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }

        return new byte[0];
    }
}
