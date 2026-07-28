
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An {@link OffsetService} decorator that seeks newly assigned partitions to the beginning and
 * captures end offsets for the catch-up phase.
 *
 * <p>This service wraps a delegate {@code OffsetService} (typically a {@link CommitOffsetService})
 * and adds seek-to-beginning behavior on partition assignment. Only partitions that have never been
 * seen before are seeked, ensuring that re-assigned partitions resume from their committed offsets
 * during the tail phase.
 *
 * <p>End offsets are captured on every assignment and made available via {@link
 * #getCatchUpEndOffsets()} for the blocking catch-up gate.
 *
 * <p><strong>Tail-phase behavior:</strong> this service remains registered as the rebalance
 * listener after catch-up completes. If a previously unseen partition is assigned during the tail
 * phase (e.g., the topic's partition count increases), it will be seeked to the beginning —
 * consistent with the connector's "full state materialization" philosophy.
 *
 * @see OffsetService
 * @see CommitOffsetService
 */
final class SeekingOffsetService implements OffsetService {

    private final OffsetService delegate;
    private final Consumer<?, ?> consumer;
    private final Logger logger;

    // Accessed exclusively from the consumer poll thread (rebalance callbacks).
    private final Set<TopicPartition> knownPartitions = new HashSet<>();

    // Written in onPartitionsAssigned and read in getCatchUpEndOffsets, both on the consumer
    // poll thread — no synchronization required.
    private Map<TopicPartition, Long> endOffsets;

    /**
     * Creates a new instance that decorates the given {@link OffsetService} with seek-to-beginning
     * behavior for previously unseen partitions and end-offset capture for the catch-up gate.
     *
     * @param delegate the {@link OffsetService} to decorate; all callbacks other than {@link
     *     #onPartitionsAssigned(Collection)} and {@link #getCatchUpEndOffsets()} are forwarded
     *     verbatim
     * @param consumer the underlying Kafka {@link Consumer} used to perform the initial seek and to
     *     fetch end offsets on each assignment
     * @param logger the {@link Logger} used for lifecycle tracing
     */
    SeekingOffsetService(OffsetService delegate, Consumer<?, ?> consumer, Logger logger) {
        this.delegate = delegate;
        this.consumer = consumer;
        this.logger = logger;
    }

    /**
     * {@inheritDoc}
     *
     * <p>For every partition seen for the first time, seeks to the beginning so the catch-up phase
     * replays the full topic history; already-known (re-assigned) partitions resume from their
     * current position. Captures end offsets for the entire current assignment so {@link
     * #getCatchUpEndOffsets()} can drive the catch-up gate.
     *
     * <p><strong>Threading note:</strong> the {@code knownPartitions} set and the {@code
     * endOffsets} field are consumer-poll-thread only; no synchronization is applied.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Determine which partitions are being seen for the first time.
        // Already-known partitions are not re-seeked, so they resume from their current position.
        Set<TopicPartition> newPartitions = new HashSet<>(partitions);
        newPartitions.removeAll(knownPartitions);

        if (!newPartitions.isEmpty()) {
            // Seek new partitions to the beginning to ensure we consume all records
            // during the catch-up phase.
            logger.atInfo().log("Seeking new partitions to beginning: {}", newPartitions);
            consumer.seekToBeginning(newPartitions);
            knownPartitions.addAll(newPartitions);
        }

        // Capture end offsets for the entire current assignment. The catch-up gate uses these
        // to know when the consumer has read up to the "live" boundary.
        endOffsets = consumer.endOffsets(partitions);
        logger.atInfo().log("Captured end offsets for current assignment: {}", endOffsets);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        delegate.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        delegate.onPartitionsLost(partitions);
    }

    @Override
    public void onConsumerShutdown() {
        delegate.onConsumerShutdown();
    }

    @Override
    public void maybeCommit() {
        delegate.maybeCommit();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
        return delegate.offsetsSnapshot();
    }

    @Override
    public void updateOffsets(KafkaRecord<?, ?> record) {
        delegate.updateOffsets(record);
    }

    @Override
    public void onAsyncFailure(Throwable th) {
        delegate.onAsyncFailure(th);
    }

    @Override
    public Throwable getFirstFailure() {
        return delegate.getFirstFailure();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the end offsets captured during the most recent {@link
     * #onPartitionsAssigned(Collection)} call, or {@code null} if no assignment has occurred yet.
     *
     * <p><strong>Threading note:</strong> this method must be called from the consumer poll thread
     * only, as the underlying field is not synchronized.
     */
    @Override
    public Map<TopicPartition, Long> getCatchUpEndOffsets() {
        return endOffsets;
    }
}
