
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

import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Represents a batch of Kafka records with flexible deserialization and processing strategies.
 *
 * <p>This interface provides an abstraction over a collection of {@link KafkaRecord}s and supports
 * both eager and deferred deserialization strategies through factory methods:
 *
 * <ul>
 *   <li><b>Eager deserialization:</b> Key/value decoding is performed immediately during batch
 *       creation via {@link #batchFromEager}. Allows early error detection but requires more
 *       upfront processing.
 *   <li><b>Deferred deserialization:</b> Key/value decoding is delayed until individual records are
 *       accessed via {@link #batchFromDeferred}. Reduces upfront cost but extends object lifetimes.
 * </ul>
 *
 * <p><b>Lifecycle:</b>
 *
 * <ol>
 *   <li>Create batch via factory method ({@code batchFromEager} or {@code batchFromDeferred})
 *   <li>Distribute records to worker threads for processing
 *   <li>Each worker calls {@link #recordProcessed(RecordBatchListener)} after processing a record
 *   <li>When all records are processed, the listener is notified
 *   <li>Optionally call {@link #join()} to block until completion
 * </ol>
 *
 * <p><b>Thread Safety:</b> Batches are created in the consumer thread and distributed to worker
 * threads for processing. The {@link #recordProcessed(RecordBatchListener)} method must be
 * thread-safe for concurrent calls from multiple workers.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see NotifyingRecordBatch
 * @see JoinableRecordBatch
 * @see KafkaRecord
 */
public interface RecordBatch<K, V> {

    /**
     * Callback interface for batch completion notifications.
     *
     * <p>Implementations are notified when all records in a batch have been processed.
     */
    interface RecordBatchListener {

        /**
         * Called when all records in a batch have been processed.
         *
         * @param batch the completed batch
         */
        void onBatchComplete(RecordBatch<?, ?> batch);

        default void checkRingBufferUtilization(
                BlockingQueue<?>[] ringBuffers, int ringBufferCapacity) {}
    }

    /**
     * Returns the total number of records in this batch.
     *
     * @return the number of records in this batch
     */
    int count();

    /**
     * Notifies that a record from this batch has been processed.
     *
     * <p>This method is called by worker threads as they complete processing individual records.
     * Implementations must handle concurrent calls safely. The listener is invoked when all records
     * in the batch have been processed.
     *
     * @param listener the completion listener to invoke when all records are processed
     */
    void recordProcessed(RecordBatchListener listener);

    /**
     * Checks whether this batch is empty.
     *
     * @return {@code true} if this batch contains no records, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Returns the list of all records in this batch.
     *
     * @return a list of {@link KafkaRecord}s in this batch
     */
    List<KafkaRecord<K, V>> getRecords();

    /**
     * Waits for all records in this batch to be processed.
     *
     * <p>This method blocks until all records have been processed. It should be called after all
     * records have been distributed to worker threads. Some implementations may not support
     * synchronous waiting.
     *
     * <p>Default implementation returns immediately.
     *
     * @throws RuntimeException if interrupted while waiting
     */
    default void join() {}

    /**
     * Validates that the batch was constructed correctly.
     *
     * <p>This method should be called after all records have been added to the batch and before the
     * batch is distributed for processing. It verifies that the actual number of records matches
     * the expected count provided at construction time.
     *
     * @throws IllegalStateException if the batch is in an invalid state (e.g., record count
     *     mismatch)
     */
    void validate();

    /**
     * Converts a batch of Kafka consumer records to a {@code RecordBatch} with eager
     * deserialization.
     *
     * <p>Deserialization is performed immediately during batch creation, which allows early error
     * detection but requires more upfront processing for all records. The returned batch does not
     * support synchronous waiting via {@link #join()}.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param consumerRecords the consumer records batch to convert
     * @param deserializerPair the pair of deserializers for keys and values
     * @return a non-joinable {@code RecordBatch} with eagerly deserialized keys and values
     * @see #batchFromEager(ConsumerRecords, DeserializerPair, boolean)
     * @see KafkaRecord#fromEager(ConsumerRecord, DeserializerPair, RecordBatch)
     */
    static <K, V> RecordBatch<K, V> batchFromEager(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            DeserializerPair<K, V> deserializerPair) {
        return batchFromEager(consumerRecords, deserializerPair, false);
    }

    /**
     * Converts a batch of Kafka consumer records to a {@code RecordBatch} with eager
     * deserialization.
     *
     * <p>Deserialization is performed immediately during batch creation. The {@code joinable}
     * parameter controls whether the returned batch supports synchronous waiting via {@link
     * #join()}.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param consumerRecords the consumer records batch to convert
     * @param deserializerPair the pair of deserializers for keys and values
     * @param joinable if {@code true}, the returned batch supports {@link #join()}; if {@code
     *     false}, {@link #join()} returns immediately
     * @return a {@code RecordBatch} with eagerly deserialized keys and values
     * @see KafkaRecord#fromEager(ConsumerRecord, DeserializerPair, RecordBatch)
     */
    static <K, V> RecordBatch<K, V> batchFromEager(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            DeserializerPair<K, V> deserializerPair,
            boolean joinable) {

        int recordCount = consumerRecords.count();
        NotifyingRecordBatch<K, V> batch =
                (joinable
                        ? new JoinableRecordBatch<>(recordCount)
                        : new NotifyingRecordBatch<>(recordCount));
        for (TopicPartition partition : consumerRecords.partitions()) {
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords.records(partition)) {
                batch.addEagerRecord(record, deserializerPair);
            }
        }
        // Validate batch construction before returning
        batch.validate();

        return batch;
    }

    /**
     * Converts a batch of Kafka consumer records to a {@code RecordBatch} with deferred
     * deserialization.
     *
     * <p>Deserialization is performed lazily when individual records are accessed, and results are
     * cached for subsequent accesses. This approach reduces upfront processing cost but extends
     * object lifetimes in memory. The returned batch does not support synchronous waiting via
     * {@link #join()}.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param consumerRecords the consumer records batch to convert
     * @param deserializerPair the pair of deserializers for keys and values
     * @return a non-joinable {@code RecordBatch} with deferred deserialization of keys and values
     * @see #batchFromDeferred(ConsumerRecords, DeserializerPair, boolean)
     * @see KafkaRecord#fromDeferred(ConsumerRecord, DeserializerPair, RecordBatch)
     */
    static <K, V> RecordBatch<K, V> batchFromDeferred(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            KafkaRecord.DeserializerPair<K, V> deserializerPair) {
        return batchFromDeferred(consumerRecords, deserializerPair, false);
    }

    /**
     * Converts a batch of Kafka consumer records to a {@code RecordBatch} with deferred
     * deserialization.
     *
     * <p>Deserialization is performed lazily when individual records are accessed. The {@code
     * joinable} parameter controls whether the returned batch supports synchronous waiting via
     * {@link #join()}.
     *
     * @param <K> the type of the deserialized key
     * @param <V> the type of the deserialized value
     * @param consumerRecords the consumer records batch to convert
     * @param deserializerPair the pair of deserializers for keys and values
     * @param joinable if {@code true}, the returned batch supports {@link #join()}; if {@code
     *     false}, {@link #join()} returns immediately
     * @return a {@code RecordBatch} with deferred deserialization of keys and values
     * @see KafkaRecord#fromDeferred(ConsumerRecord, DeserializerPair, RecordBatch)
     */
    static <K, V> RecordBatch<K, V> batchFromDeferred(
            ConsumerRecords<byte[], byte[]> consumerRecords,
            KafkaRecord.DeserializerPair<K, V> deserializerPair,
            boolean joinable) {
        int recordCount = consumerRecords.count();
        NotifyingRecordBatch<K, V> batch =
                joinable
                        ? new JoinableRecordBatch<>(recordCount)
                        : new NotifyingRecordBatch<>(recordCount);
        for (TopicPartition partition : consumerRecords.partitions()) {
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords.records(partition)) {
                batch.addDeferredRecord(record, deserializerPair);
            }
        }
        // Validate batch construction before returning
        batch.validate();

        return batch;
    }
}
