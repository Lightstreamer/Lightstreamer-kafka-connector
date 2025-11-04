
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

final record KafkaRecordHeader(String key, byte[] value) implements KafkaHeader {

    KafkaRecordHeader(org.apache.kafka.common.header.Header header) {
        this(header.key(), header.value());
    }

    KafkaRecordHeader(org.apache.kafka.connect.header.Header header) {
        this(header.key(), checkValue(header.value()));
    }

    private static byte[] checkValue(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }

        return new byte[0];
    }
}
