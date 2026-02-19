
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation of {@link RecordBatch} that notifies a listener upon batch completion.
 *
 * <p>This implementation tracks record processing via an {@link AtomicInteger} counter and invokes
 * a completion listener when all records have been processed. It does not support synchronous
 * waiting via {@link #join()}.
 *
 * <p><b>Use case:</b> High-throughput asynchronous processing where completion is signaled via
 * listener notification rather than blocking waits. Lower synchronization overhead than {@link
 * JoinableRecordBatch}.
 *
 * <p><b>Thread Safety:</b> Safe for concurrent calls to {@link
 * #recordProcessed(RecordBatch.RecordBatchListener)} from multiple worker threads.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see JoinableRecordBatch
 */
public class NotifyingRecordBatch<K, V> implements RecordBatch<K, V> {

    private final List<KafkaRecord<K, V>> records;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final int recordCount;

    /**
     * Constructs a new {@code NotifyingRecordBatch} with the specified capacity.
     *
     * @param size the expected number of records in this batch
     */
    public NotifyingRecordBatch(int size) {
        this.records = new ArrayList<>(size);
        this.recordCount = size;
    }

    /**
     * Adds a record with eager deserialization to this batch.
     *
     * <p>The record's key and value are deserialized immediately.
     *
     * @param record the raw Kafka consumer record
     * @param deserializerPair the deserializers for key and value
     */
    void addEagerRecord(
            ConsumerRecord<byte[], byte[]> record,
            KafkaRecord.DeserializerPair<K, V> deserializerPair) {
        this.records.add(KafkaRecord.fromEager(record, deserializerPair, this));
    }

    /**
     * Adds a record with deferred deserialization to this batch.
     *
     * <p>The record's key and value are deserialized lazily on first access.
     *
     * @param record the raw Kafka consumer record
     * @param deserializerPair the deserializers for key and value
     */
    void addDeferredRecord(
            ConsumerRecord<byte[], byte[]> record,
            KafkaRecord.DeserializerPair<K, V> deserializerPair) {
        this.records.add(KafkaRecord.fromDeferred(record, deserializerPair, this));
    }

    @Override
    public List<KafkaRecord<K, V>> getRecords() {
        return records;
    }

    @Override
    public boolean isEmpty() {
        return recordCount == 0;
    }

    @Override
    public int count() {
        return recordCount;
    }

    @Override
    public void validate() {
        if (records.size() != recordCount) {
            throw new IllegalStateException(
                    "Expected " + recordCount + " records but got " + records.size());
        }
    }

    @Override
    public void recordProcessed(RecordBatchListener monitor) {
        if (processedCount.incrementAndGet() == recordCount) {
            monitor.onBatchComplete(this);
        }
    }
}
