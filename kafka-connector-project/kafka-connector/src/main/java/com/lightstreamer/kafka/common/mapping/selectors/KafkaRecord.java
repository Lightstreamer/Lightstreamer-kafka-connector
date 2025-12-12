
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public interface KafkaRecord<K, V> {

    public interface KafkaHeader {

        String key();

        byte[] value();

        default int localIndex() {
            return -1;
        }
    }

    public interface KafkaHeaders extends Iterable<KafkaHeader> {

        boolean has(String key);

        KafkaHeader get(int index);

        List<KafkaHeader> headers(String key);

        int size();
    }

    public static <K, V> KafkaRecord<K, V> from(ConsumerRecord<K, V> record) {
        return new KafkaConsumerRecord<>(record);
    }

    public static KafkaRecord<Object, Object> from(SinkRecord record) {
        return new KafkaSinkRecord(record);
    }

    K key();

    V value();

    long timestamp();

    long offset();

    String topic();

    int partition();

    KafkaHeaders headers();

    final class KafkaConsumerRecord<K, V> implements KafkaRecord<K, V> {

        private final ConsumerRecord<K, V> record;

        private KafkaConsumerRecord(ConsumerRecord<K, V> record) {
            this.record = record;
        }

        @Override
        public K key() {
            return record.key();
        }

        @Override
        public V value() {
            return record.value();
        }

        @Override
        public long timestamp() {
            return record.timestamp();
        }

        @Override
        public long offset() {
            return record.offset();
        }

        @Override
        public String topic() {
            return record.topic();
        }

        @Override
        public int partition() {
            return record.partition();
        }

        @Override
        public KafkaHeaders headers() {
            return new KafkaHeadersImpl(HeadersAdapter.from(record.headers()));
        }
    }

    final class KafkaSinkRecord implements KafkaRecord<Object, Object> {

        private final SinkRecord record;

        private KafkaSinkRecord(SinkRecord record) {
            this.record = record;
        }

        @Override
        public Object key() {
            return record.key();
        }

        @Override
        public Object value() {
            return record.value();
        }

        @Override
        public long timestamp() {
            return record.timestamp();
        }

        @Override
        public long offset() {
            return record.kafkaOffset();
        }

        @Override
        public String topic() {
            return record.topic();
        }

        @Override
        public int partition() {
            return record.kafkaPartition();
        }

        public SchemaAndValue keySchemaAndValue() {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        public SchemaAndValue valueSchemaAndValue() {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        public KafkaHeaders headers() {
            return new KafkaHeadersImpl(HeadersAdapter.from(record.headers()));
        }
    }

    final record KafkaHeaderImpl(String key, byte[] value, int localIndex) implements KafkaHeader {

        private KafkaHeaderImpl(HeaderAdapter header) {
            this(header, -1);
        }

        private KafkaHeaderImpl(HeaderAdapter header, int localIndex) {
            this(header.key(), header.value(), localIndex);
        }
    }

    interface HeadersAdapter extends Iterable<HeaderAdapter> {

        static HeadersAdapter from(Headers headers) {
            return new HeadersAdapter() {

                @Override
                public Iterator<HeaderAdapter> iterator() {
                    return StreamSupport.stream(headers.spliterator(), false)
                            .map(HeaderAdapter::from)
                            .iterator();
                }
            };
        }

        static HeadersAdapter from(org.apache.kafka.connect.header.Headers headers) {
            return new HeadersAdapter() {

                @Override
                public Iterator<HeaderAdapter> iterator() {
                    return StreamSupport.stream(headers.spliterator(), false)
                            .map(HeaderAdapter::from)
                            .iterator();
                }
            };
        }
    }

    interface HeaderAdapter {

        String key();

        byte[] value();

        static HeaderAdapter from(Header header) {
            return new HeaderAdapter() {

                @Override
                public String key() {
                    return header.key();
                }

                @Override
                public byte[] value() {
                    return header.value();
                }
            };
        }

        static HeaderAdapter from(org.apache.kafka.connect.header.Header header) {
            return new HeaderAdapter() {

                @Override
                public String key() {
                    return header.key();
                }

                @Override
                public byte[] value() {
                    return checkValue(header.value());
                }

                private static byte[] checkValue(Object value) {
                    if (value instanceof byte[] bytes) {
                        return bytes;
                    }

                    return new byte[0];
                }
            };
        }
    }

    static class HeaderReference {

        private int totalRefs = -1;
        private int currentRef = 0;

        HeaderReference() {}

        void incTotalRefs() {
            if (totalRefs == -1) {
                totalRefs = 1;
            }
            totalRefs++;
        }

        int getAndIncrement() {
            return currentRef++;
        }
    }

    static final class KafkaHeadersImpl implements KafkaHeaders {

        private final List<KafkaHeader> headersArray = new ArrayList<>();
        private final Map<String, List<KafkaHeader>> headersMap = new HashMap<>();

        KafkaHeadersImpl(Headers headers) {
            this(HeadersAdapter.from(headers));
        }

        KafkaHeadersImpl(org.apache.kafka.connect.header.Headers headers) {
            this(HeadersAdapter.from(headers));
        }

        KafkaHeadersImpl(HeadersAdapter headers) {
            Map<String, HeaderReference> refs = new HashMap<>();
            for (HeaderAdapter header : headers) {
                refs.compute(
                        header.key(),
                        (k, v) -> {
                            if (v == null) {
                                return new HeaderReference();
                            } else {
                                v.incTotalRefs();
                            }
                            return v;
                        });
            }

            for (HeaderAdapter header : headers) {
                List<KafkaHeader> list =
                        headersMap.computeIfAbsent(header.key(), k -> new ArrayList<>());
                HeaderReference ref = refs.get(header.key());
                KafkaHeaderImpl kafkaRecordHeader;
                if (ref.totalRefs == -1) {
                    kafkaRecordHeader = new KafkaHeaderImpl(header);
                } else {
                    kafkaRecordHeader = new KafkaHeaderImpl(header, ref.getAndIncrement());
                }
                list.add(kafkaRecordHeader);
                headersArray.add(kafkaRecordHeader);
            }
        }

        @Override
        public boolean has(String key) {
            return headersMap.containsKey(key);
        }

        @Override
        public KafkaHeader get(int index) {
            return headersArray.get(index);
        }

        @Override
        public List<KafkaHeader> headers(String key) {
            return headersMap.get(key);
        }

        @Override
        public int size() {
            return headersArray.size();
        }

        @Override
        public Iterator<KafkaHeader> iterator() {
            return headersArray.iterator();
        }
    }
}
