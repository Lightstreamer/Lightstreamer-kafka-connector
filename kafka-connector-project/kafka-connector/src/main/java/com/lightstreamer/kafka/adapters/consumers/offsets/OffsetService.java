
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

import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

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
     * Creates a new {@code OffsetService} for the specified consumer.
     *
     * @param consumer the Kafka {@link Consumer} to commit offsets to
     * @param logger the {@link Logger} for commit diagnostics
     * @return a new {@code OffsetService} instance
     */
    static OffsetService create(Consumer<?, ?> consumer, Logger logger) {
        return new OffsetServiceImpl(consumer, logger);
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
     * Returns the first asynchronous failure recorded, if any.
     *
     * @return the first failure, or {@code null} if no failures have occurred
     */
    Throwable getFirstFailure();

    /**
     * Returns an unmodifiable snapshot of the currently tracked offsets.
     *
     * @return an unmodifiable copy of the offset map
     */
    Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot();
}
