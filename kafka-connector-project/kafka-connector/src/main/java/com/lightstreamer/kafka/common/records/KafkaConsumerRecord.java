
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An abstract sealed base class for {@link KafkaRecord} implementations based on Kafka consumer
 * records.
 *
 * <p>This class provides common functionality for converting Kafka consumer records to {@link
 * KafkaRecord} instances. Subclasses determine when deserialization of key and value occurs
 * (eagerly vs. deferred).
 *
 * <p>The class is sealed and permits only {@link EagerKafkaConsumerRecord} and {@link
 * DeferredKafkaConsumerRecord} as subclasses.
 *
 * @param <K> the type of the deserialized key
 * @param <V> the type of the deserialized value
 * @see EagerKafkaConsumerRecord
 * @see DeferredKafkaConsumerRecord
 */
abstract sealed class KafkaConsumerRecord<K, V> implements KafkaRecord<K, V>
        permits EagerKafkaConsumerRecord, DeferredKafkaConsumerRecord {

    /** The underlying Kafka consumer record with byte array key and value. */
    protected final ConsumerRecord<byte[], byte[]> record;

    protected final RecordBatch<K, V> batch;

    /**
     * Constructs a {@link KafkaConsumerRecord} with the given consumer record.
     *
     * @param record the raw Kafka consumer record
     * @param batch the batch this record belongs to
     */
    KafkaConsumerRecord(ConsumerRecord<byte[], byte[]> record, RecordBatch<K, V> batch) {
        this.record = record;
        this.batch = batch;
    }

    @Override
    public final long timestamp() {
        return record.timestamp();
    }

    @Override
    public final long offset() {
        return record.offset();
    }

    @Override
    public final String topic() {
        return record.topic();
    }

    @Override
    public final int partition() {
        return record.partition();
    }

    @Override
    public final KafkaHeaders headers() {
        return KafkaHeaders.from(record.headers());
    }

    @Override
    public final RecordBatch<K, V> getBatch() {
        return batch;
    }
}
