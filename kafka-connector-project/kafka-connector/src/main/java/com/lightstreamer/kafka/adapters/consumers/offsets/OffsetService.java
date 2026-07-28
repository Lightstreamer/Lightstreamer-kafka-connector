
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * Manages Kafka consumer offset tracking and commit lifecycle.
 *
 * <p>An {@code OffsetService} tracks offsets for consumed records and commits them back to Kafka
 * either synchronously (on rebalance or shutdown) or asynchronously (during normal processing). It
 * also acts as a {@link ConsumerRebalanceListener} to handle partition assignment changes.
 */
public interface OffsetService extends ConsumerRebalanceListener {

    /**
     * Creates a new {@code OffsetService} that tracks and commits offsets to Kafka.
     *
     * @param consumer the Kafka {@link Consumer} to commit offsets to
     * @param logger the {@link Logger} for commit diagnostics
     * @return a new committing {@code OffsetService} instance
     */
    static OffsetService commit(Consumer<?, ?> consumer, Logger logger) {
        return new CommitOffsetService(consumer, logger);
    }

    /**
     * Creates a no-operation {@code OffsetService} that never commits offsets.
     *
     * <p>Suitable for consumers using manual partition assignment ({@code assign()}) where offset
     * tracking is unnecessary. On each partition assignment, the returned service seeks the newly
     * assigned partitions to the position dictated by {@code consumeFrom}.
     *
     * @param consumer the Kafka {@link Consumer} to seek on partition assignment
     * @param logger the {@link Logger} for lifecycle diagnostics
     * @param consumeFrom the {@link RecordConsumeFrom} dictating the initial-seek position for each
     *     newly assigned partition ({@link RecordConsumeFrom#EARLIEST} for the beginning, {@link
     *     RecordConsumeFrom#LATEST} for the end)
     * @return a no-op {@code OffsetService} instance
     */
    static OffsetService noCommit(
            Consumer<?, ?> consumer, Logger logger, RecordConsumeFrom consumeFrom) {
        return new NoCommitOffsetService(consumer, logger, consumeFrom);
    }

    /**
     * Creates an {@code OffsetService} that decorates the given delegate with seek-to-beginning
     * behavior on partition assignment and captures end offsets for the catch-up gate.
     *
     * <p>On each partition assignment, partitions that have never been seen before are seeked to
     * the beginning; re-assigned partitions resume from their committed offsets. End offsets are
     * captured for the entire current assignment and made available via {@link
     * #getCatchUpEndOffsets()}.
     *
     * @param delegate the {@code OffsetService} to decorate; all callbacks other than {@link
     *     ConsumerRebalanceListener#onPartitionsAssigned(Collection)} and {@link
     *     #getCatchUpEndOffsets()} are forwarded verbatim
     * @param consumer the Kafka {@link Consumer} to seek on partition assignment
     * @param logger the {@link Logger} for seek diagnostics
     * @return a seeking {@code OffsetService} instance
     */
    static OffsetService seekingCommit(
            OffsetService delegate, Consumer<?, ?> consumer, Logger logger) {
        return new SeekingOffsetService(delegate, consumer, logger);
    }

    /**
     * Returns an unmodifiable snapshot of the currently tracked offsets.
     *
     * @return an unmodifiable copy of the offset map
     */
    Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot();

    /**
     * Returns the first asynchronous failure recorded, if any.
     *
     * @return the first failure, or {@code null} if no failures have occurred
     */
    Throwable getFirstFailure();

    /**
     * Returns the end offsets captured during the most recent partition assignment, for use as the
     * catch-up completion gate.
     *
     * <p>The default implementation returns {@code null}, indicating that no catch-up end offsets
     * are tracked. Implementations that seek to the beginning on assignment override this to
     * provide the target offsets.
     *
     * @return the end offsets map, or {@code null} if not applicable
     */
    default Map<TopicPartition, Long> getCatchUpEndOffsets() {
        return null;
    }

    /**
     * Commits offsets asynchronously if the current {@link CommitStrategy} determines it is time.
     */
    void maybeCommit();

    /**
     * Tracks the offset of the given record for later commit.
     *
     * @param record the {@link KafkaRecord} whose offset to track
     */
    void updateOffsets(KafkaRecord<?, ?> record);

    /**
     * Records an asynchronous processing failure.
     *
     * @param th the failure cause
     */
    void onAsyncFailure(Throwable th);

    /** Signals that the consumer is shutting down and performs a final synchronous commit. */
    void onConsumerShutdown();

    /**
     * Called when partitions are lost during a consumer group rebalance.
     *
     * <p>The default implementation delegates to {@link #onPartitionsRevoked(Collection)} as a
     * convenience for implementations that treat "lost" partitions the same as "revoked" ones.
     *
     * @param partitions the partitions that were lost
     */
    @Override
    default void onPartitionsLost(Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}
