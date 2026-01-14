
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

import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeader;
import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeaders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

class KafkaHeadersImpl implements KafkaHeaders {

    private final List<KafkaHeader> headersArray = new ArrayList<>();
    private final Map<String, List<KafkaHeader>> headersMap = new HashMap<>();

    KafkaHeadersImpl(org.apache.kafka.connect.header.Headers headers) {
        buildHeaders(headers, org.apache.kafka.connect.header.Header::key, KafkaRecordHeader::new);
    }

    KafkaHeadersImpl(org.apache.kafka.common.header.Headers headers) {
        buildHeaders(headers, org.apache.kafka.common.header.Header::key, KafkaRecordHeader::new);
    }

    protected final <H> void buildHeaders(
            Iterable<H> headers,
            Function<H, String> keyExtractor,
            BiFunction<H, Integer, KafkaRecordHeader> headerFactory) {
        // First pass: count occurrences using int[] to store [count, currentIndex]
        Map<String, int[]> tracker = new HashMap<>();
        List<H> headerList = new ArrayList<>();
        for (H header : headers) {
            headerList.add(header);
            tracker.computeIfAbsent(keyExtractor.apply(header), k -> new int[2])[0]++;
        }

        // Second pass: build headers with proper indexing
        for (H header : headerList) {
            String key = keyExtractor.apply(header);
            int[] counts = tracker.get(key);
            int index = (counts[0] == 1) ? -1 : counts[1]++;

            KafkaRecordHeader kafkaRecordHeader = headerFactory.apply(header, index);
            headersMap.computeIfAbsent(key, k -> new ArrayList<>()).add(kafkaRecordHeader);
            headersArray.add(kafkaRecordHeader);
        }
    }

    @Override
    public final boolean has(String key) {
        return headersMap.containsKey(key);
    }

    @Override
    public final KafkaHeader get(int index) {
        return headersArray.get(index);
    }

    @Override
    public final List<KafkaHeader> headers(String key) {
        return headersMap.get(key);
    }

    @Override
    public final int size() {
        return headersArray.size();
    }

    @Override
    public final Iterator<KafkaHeader> iterator() {
        return headersArray.iterator();
    }
}
