
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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface KafkaRecord<K, V> {

    public interface KafkaHeader {

        String key();

        byte[] value();
    }

    public interface KafkaHeaders extends Iterable<KafkaHeader> {

        KafkaHeader lastHeader(String key);

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
            return new KafkaRecordHeaders(record.headers());
        }
    }

    final record KafkaRecordHeader(String key, byte[] value) implements KafkaHeader {

        private KafkaRecordHeader(Header header) {
            this(header.key(), header.value());
        }

        private KafkaRecordHeader(org.apache.kafka.connect.header.Header header) {
            this(header.key(), checkValue(header.value()));
        }

        private static byte[] checkValue(Object value) {
            if (value instanceof byte[] bytes) {
                return bytes;
            }

            return new byte[0];
        }
    }

    final class KafkaRecordHeaders implements KafkaHeaders {

        private final Headers headers;
        private final List<Header> headersArray;

        KafkaRecordHeaders(Headers headers) {
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

        public Schema keySchema() {
            return record.keySchema();
        }

        public Schema valueSchema() {
            return record.valueSchema();
        }

        @Override
        public KafkaHeaders headers() {
            return new KafkaConnectHeaders(record.headers());
        }
    }

    final class KafkaConnectHeaders implements KafkaHeaders {

        private final org.apache.kafka.connect.header.Headers headers;
        private final List<org.apache.kafka.connect.header.Header> headersArray;

        KafkaConnectHeaders(org.apache.kafka.connect.header.Headers headers) {
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
}
