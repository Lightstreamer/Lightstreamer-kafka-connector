
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

import java.util.function.Predicate;

public interface KafkaRecords<K, V> extends Iterable<KafkaRecord<K, V>> {

    static <K, V> KafkaRecords<K, V> from(ConsumerRecords<Deferred<K>, Deferred<V>> records) {
        return new KafkaConsumerRecords<>(records);
    }

    KafkaRecords<K, V> filter(Predicate<KafkaRecord<K, V>> predicate);

    int count();
}
