
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * {@link OffsetService} implementation for {@code consumer.mode = MANUAL}: never commits offsets to
 * Kafka, and seeks each newly assigned partition to the position dictated by the connection's
 * {@link RecordConsumeFrom} (beginning for {@code EARLIEST}, end for {@code LATEST}).
 *
 * <p>All commit-related lifecycle methods are no-ops (there is no {@code group.id} in MANUAL mode
 * and therefore no {@code __consumer_offsets} target to write to). The {@link
 * org.apache.kafka.clients.consumer.ConsumerRebalanceListener} callbacks are not invoked by the
 * Kafka client under manual assignment; {@code onPartitionsAssigned} is instead invoked directly by
 * the consumer wrapper after {@code assign()} to perform the initial seek. The {@code
 * onPartitionsRevoked} and {@code onPartitionsLost} callbacks remain wired for defence in depth and
 * log partition changes only.
 *
 * @see CommitOffsetService
 * @see OffsetService
 */
final class NoCommitOffsetService implements OffsetService {

    private final Consumer<?, ?> consumer;
    private final Logger logger;
    private final RecordConsumeFrom consumeFrom;

    /**
     * Creates a new instance that seeks each assigned partition per the supplied {@link
     * RecordConsumeFrom}.
     *
     * @param consumer the underlying Kafka {@link Consumer} against which the initial seek is
     *     performed on partition assignment
     * @param logger the {@link Logger} used for lifecycle tracing
     * @param consumeFrom the {@link RecordConsumeFrom} dictating the initial-seek position for each
     *     newly assigned partition ({@link RecordConsumeFrom#EARLIEST} for the beginning, {@link
     *     RecordConsumeFrom#LATEST} for the end)
     */
    NoCommitOffsetService(Consumer<?, ?> consumer, Logger logger, RecordConsumeFrom consumeFrom) {
        this.consumer = consumer;
        this.logger = logger;
        this.consumeFrom = consumeFrom;
    }

    /**
     * {@inheritDoc}
     *
     * <p>In addition to the interface contract, seeks the assigned partitions to the position
     * dictated by the connection's {@link RecordConsumeFrom} (beginning for {@code EARLIEST}, end
     * for {@code LATEST}). This method is invoked directly by the consumer wrapper after {@code
     * assign()} in MANUAL mode, since no rebalance protocol runs.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.atInfo().log("Assigned partitions {}", partitions);
        switch (consumeFrom) {
            case EARLIEST -> {
                logger.atInfo().log("Seeking assigned partitions to beginning: {}", partitions);
                consumer.seekToBeginning(partitions);
            }
            case LATEST -> {
                logger.atInfo().log("Seeking assigned partitions to end: {}", partitions);
                consumer.seekToEnd(partitions);
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.atWarn().log("Unexpected onPartitionsRevoked in MANUAL mode: {}", partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.atWarn().log("Unexpected onPartitionsLost in MANUAL mode: {}", partitions);
    }

    @Override
    public void onConsumerShutdown() {
        logger.atDebug().log("Consumer shutdown — no offsets to commit");
    }

    @Override
    public void maybeCommit() {
        // No-op: offsets are never committed
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
        return Collections.emptyMap();
    }

    @Override
    public void updateOffsets(KafkaRecord<?, ?> record) {
        // No-op: offsets are not tracked
    }

    @Override
    public void onAsyncFailure(Throwable th) {
        logger.atWarn().setCause(th).log("Async failure reported to no-commit offset service");
    }

    @Override
    public Throwable getFirstFailure() {
        return null;
    }
}
