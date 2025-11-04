
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

import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class KafkaConsumerRecords<K, V> implements KafkaRecords<K, V> {

    private ConsumerRecords<Deferred<K>, Deferred<V>> consumerRecords;

    KafkaConsumerRecords(ConsumerRecords<Deferred<K>, Deferred<V>> consumerRecords) {
        this.consumerRecords = consumerRecords;
    }

    @Override
    public int count() {
        return consumerRecords.count();
    }

    @Override
    public Stream<KafkaRecord<K, V>> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public Iterator<KafkaRecord<K, V>> iterator() {
        Iterator<ConsumerRecord<Deferred<K>, Deferred<V>>> iterator = consumerRecords.iterator();
        return new Iterator<KafkaRecord<K, V>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KafkaRecord<K, V> next() {
                return KafkaRecord.from(iterator.next());
            }
        };
    }
}
