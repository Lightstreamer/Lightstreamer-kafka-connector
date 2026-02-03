
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
 * Callback-based implementation of {@link RecordBatch} with asynchronous completion callbacks.
 *
 * <p>This implementation tracks record processing via an {@link AtomicInteger} counter and invokes
 * a completion listener callback when all records have been processed. It does not support
 * synchronous waiting via {@link #join()}.
 *
 * <p><b>Use case:</b> High-throughput asynchronous processing where completion is signaled via
 * callbacks rather than blocking waits. Lower synchronization overhead than {@link
 * JoinableRecordBatch}.
 *
 * <p><b>Thread Safety:</b> Safe for concurrent calls to {@link
 * #recordProcessed(RecordBatchListener)} from multiple worker threads.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see JoinableRecordBatch
 */
public class CallbackRecordBatch<K, V> implements RecordBatch<K, V> {

    private final List<KafkaRecord<K, V>> records;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final int recordCount;

    /**
     * Constructs a new {@code CallbackRecordBatch} with the given records.
     *
     * @param records the list of {@link KafkaRecord}s in this batch
     */
    public CallbackRecordBatch(int size) {
        this.records = new ArrayList<>(size);
        this.recordCount = size;
    }

    void addEagerRecord(
            ConsumerRecord<byte[], byte[]> record,
            KafkaRecord.DeserializerPair<K, V> deserializerPair) {
        this.records.add(KafkaRecord.fromEager(record, deserializerPair, this));
    }

    void addDeferredRecord(
            ConsumerRecord<byte[], byte[]> record,
            KafkaRecord.DeserializerPair<K, V> deserializerPair) {
        this.records.add(KafkaRecord.fromDeferred(record, deserializerPair, this));
    }

    /**
     * Returns the list of all records in this batch.
     *
     * @return an unmodifiable list of {@link KafkaRecord}s
     */
    @Override
    public List<KafkaRecord<K, V>> getRecords() {
        return records;
    }

    /**
     * Checks whether this batch is empty.
     *
     * @return {@code true} if this batch contains no records, {@code false} otherwise
     */
    @Override
    public boolean isEmpty() {
        return recordCount == 0;
    }

    /**
     * Returns the total number of records in this batch.
     *
     * @return the number of records
     */
    @Override
    public int count() {
        return recordCount;
    }

    /**
     * Notifies that a record from this batch has been processed.
     *
     * <p>This method is called by worker threads as they complete processing individual records.
     * When all records have been processed, the completion listener is invoked.
     *
     * @param listener the completion listener to invoke when all records are processed
     */
    @Override
    public void recordProcessed(RecordBatchListener listener) {
        if (processedCount.incrementAndGet() == count()) {
            listener.onBatchComplete(this);
        }
    }
}
