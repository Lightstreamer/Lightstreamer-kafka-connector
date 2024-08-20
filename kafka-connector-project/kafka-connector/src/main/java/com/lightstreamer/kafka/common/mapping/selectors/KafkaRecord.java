
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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public interface KafkaRecord<K, V> {

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
    }
}
