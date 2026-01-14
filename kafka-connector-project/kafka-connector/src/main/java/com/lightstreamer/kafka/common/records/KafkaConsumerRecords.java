
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

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

final class KafkaConsumerRecords<K, V> implements KafkaRecords<K, V> {

    private final List<KafkaRecord<K, V>> recordList;

    KafkaConsumerRecords(ConsumerRecords<Deferred<K>, Deferred<V>> consumerRecords) {
        this.recordList = new ArrayList<>(consumerRecords.count());
        consumerRecords.forEach(record -> recordList.add(KafkaRecord.from(record)));
    }

    KafkaConsumerRecords(List<KafkaRecord<K, V>> recordList) {
        this.recordList = recordList;
    }

    @Override
    public int count() {
        return recordList.size();
    }

    @Override
    public KafkaRecords<K, V> filter(Predicate<KafkaRecord<K, V>> predicate) {
        return new KafkaConsumerRecords<>(recordList.stream().filter(predicate).toList());
    }

    @Override
    public Iterator<KafkaRecord<K, V>> iterator() {
        return recordList.iterator();
    }
}
