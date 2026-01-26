
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

/**
 * An implementation of {@link KafkaHeaders} that manages headers from both Kafka Consumer API and
 * Kafka Connect API sources.
 *
 * <p>Headers are stored in two data structures for efficient access:
 *
 * <ul>
 *   <li>A {@code List} for sequential access and iteration
 *   <li>A {@code Map} for fast key-based lookups
 * </ul>
 *
 * <p>When headers with the same key exist, the local index tracks their position within the group
 * of headers with that key. Single-key headers have a local index of {@code -1}.
 */
class KafkaHeadersImpl implements KafkaHeaders {

    /** List of headers in order for efficient iteration. */
    private final List<KafkaHeader> headersArray = new ArrayList<>();

    /** Map from header key to list of headers with that key for efficient lookups. */
    private final Map<String, List<KafkaHeader>> headersMap = new HashMap<>();

    /**
     * Constructs a {@link KafkaHeadersImpl} from Kafka Consumer API headers.
     *
     * @param headers the Kafka Consumer API headers
     */
    KafkaHeadersImpl(org.apache.kafka.common.header.Headers headers) {
        buildHeaders(headers, org.apache.kafka.common.header.Header::key, KafkaRecordHeader::new);
    }

    /**
     * Constructs a {@link KafkaHeadersImpl} from Kafka Connect headers.
     *
     * @param headers the Kafka Connect headers
     */
    KafkaHeadersImpl(org.apache.kafka.connect.header.Headers headers) {
        buildHeaders(headers, org.apache.kafka.connect.header.Header::key, KafkaRecordHeader::new);
    }

    /**
     * Builds the headers collection from a generic header source.
     *
     * <p>This method processes headers in two passes:
     *
     * <ol>
     *   <li>First pass: Count occurrences of each key
     *   <li>Second pass: Create {@link KafkaRecordHeader} instances with proper indexing
     * </ol>
     *
     * @param <H> the type of the source header
     * @param headers the source headers
     * @param keyExtractor function to extract the key from a header
     * @param headerFactory function to create a {@link KafkaRecordHeader} from a header and index
     */
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

    /**
     * Checks whether a header with the given key exists.
     *
     * @param key the header key to check
     * @return {@code true} if a header with the given key exists, {@code false} otherwise
     */
    @Override
    public final boolean has(String key) {
        return headersMap.containsKey(key);
    }

    /**
     * Returns the header at the given index.
     *
     * @param index the index of the header
     * @return the header at the given index
     * @throws IndexOutOfBoundsException if index is out of range
     */
    @Override
    public final KafkaHeader get(int index) {
        return headersArray.get(index);
    }

    /**
     * Returns all headers with the given key.
     *
     * @param key the header key
     * @return a list of headers with the given key, or {@code null} if no headers with this key
     *     exist
     */
    @Override
    public final List<KafkaHeader> headers(String key) {
        return headersMap.get(key);
    }

    /**
     * Returns the total number of headers.
     *
     * @return the number of headers
     */
    @Override
    public final int size() {
        return headersArray.size();
    }

    /**
     * Returns an iterator over the headers in order.
     *
     * @return an iterator over the headers
     */
    @Override
    public final Iterator<KafkaHeader> iterator() {
        return headersArray.iterator();
    }
}
