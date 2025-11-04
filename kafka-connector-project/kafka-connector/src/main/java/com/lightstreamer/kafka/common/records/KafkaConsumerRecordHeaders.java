
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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class KafkaConsumerRecordHeaders implements KafkaHeaders {

    private final Headers headers;
    private final List<Header> headersArray;

    public KafkaConsumerRecordHeaders(Headers headers) {
        this.headers = headers;
        this.headersArray = new ArrayList<>();
        for (Header header : headers) {
            headersArray.add(header);
        }
    }

    @Override
    public KafkaHeader lastHeader(String key) {
        Header header = headers.lastHeader(key);
        return header != null ? new KafkaRecordHeader(header) : null;
    }

    @Override
    public KafkaHeader get(int index) {
        Header header = headersArray.get(index);
        return header != null ? new KafkaRecordHeader(header) : null;
    }

    @Override
    public List<KafkaHeader> headers(String key) {
        Iterable<Header> headersIterable = headers.headers(key);
        List<KafkaHeader> headersArray = new ArrayList<>();
        for (Header header : headersIterable) {
            headersArray.add(new KafkaRecordHeader(header));
        }

        return headersArray;
    }

    @Override
    public int size() {
        return headersArray.size();
    }

    @Override
    public Iterator<KafkaHeader> iterator() {
        return new Iterator<KafkaHeader>() {
            private final Iterator<Header> iterator = headersArray.iterator();

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
