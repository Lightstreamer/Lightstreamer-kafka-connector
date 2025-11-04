
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
import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeaders;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class KafkaConnectHeaders implements KafkaHeaders {

    private final org.apache.kafka.connect.header.Headers headers;
    private final List<org.apache.kafka.connect.header.Header> headersArray;

    public KafkaConnectHeaders(org.apache.kafka.connect.header.Headers headers) {
        this.headers = headers;
        this.headersArray = new ArrayList<>();
        for (org.apache.kafka.connect.header.Header header : headers) {
            headersArray.add(header);
        }
    }

    @Override
    public KafkaHeader lastHeader(String key) {
        org.apache.kafka.connect.header.Header header = headers.lastWithName(key);
        return header != null ? new KafkaRecordHeader(header) : null;
    }

    @Override
    public KafkaHeader get(int index) {
        org.apache.kafka.connect.header.Header header = headersArray.get(index);
        return header != null ? new KafkaRecordHeader(header) : null;
    }

    @Override
    public List<KafkaHeader> headers(String key) {
        Iterator<org.apache.kafka.connect.header.Header> iterator = headers.allWithName(key);
        List<KafkaHeader> headersArray = new ArrayList<>();
        while (iterator.hasNext()) {
            org.apache.kafka.connect.header.Header header = iterator.next();
            headersArray.add(new KafkaRecordHeader(header));
        }

        return headersArray;
    }

    @Override
    public int size() {
        return headers.size();
    }

    @Override
    public Iterator<KafkaHeader> iterator() {
        return new Iterator<KafkaHeader>() {
            private final Iterator<org.apache.kafka.connect.header.Header> iterator =
                    headers.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KafkaHeader next() {
                return new KafkaRecordHeader(iterator.next());
            }
        };
    }
}
