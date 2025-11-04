
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

package com.lightstreamer.kafka.common.records;

import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.sink.SinkRecord;

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

    public static <K, V> KafkaRecord<K, V> from(ConsumerRecord<Deferred<K>, Deferred<V>> record) {
        return new KafkaConsumerRecord<>(record);
    }

    public static KafkaRecord<Object, Object> from(SinkRecord record) {
        return new KafkaSinkRecord(record);
    }

    K key();

    V value();

    boolean isPayloadNull();

    long timestamp();

    long offset();

    String topic();

    int partition();

    KafkaHeaders headers();
}
