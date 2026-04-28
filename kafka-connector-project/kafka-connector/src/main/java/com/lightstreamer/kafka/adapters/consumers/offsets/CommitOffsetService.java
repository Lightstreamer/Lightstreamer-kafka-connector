
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link OffsetService} implementation that tracks offsets in a {@link ConcurrentHashMap} and
 * commits them to Kafka using configurable {@link CommitStrategy} timing.
 */
class CommitOffsetService implements OffsetService {

    private final Consumer<?, ?> consumer;
    private final Logger logger;
    private final CommitStrategy commitStrategy;
    private final AtomicBoolean consumerShuttingDown = new AtomicBoolean(false);
    private final AtomicInteger recordsCounter = new AtomicInteger();
    private final AtomicInteger consecutiveCommitFailures = new AtomicInteger(0);
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    private volatile Throwable firstFailure;
    private volatile long lastCommitTimeMs = System.currentTimeMillis();
    private volatile int messagesSinceLastCommit = 0;

    /** Creates a new instance with the default {@link CommitStrategy.Adaptive} strategy. */
    CommitOffsetService(Consumer<?, ?> consumer, Logger logger) {
        this(consumer, logger, CommitStrategy.adaptiveCommitStrategy(5));
    }

    /** Creates a new instance with the specified {@link CommitStrategy}. */
    CommitOffsetService(Consumer<?, ?> consumer, Logger logger, CommitStrategy commitStrategy) {
        this.consumer = consumer;
        this.logger = logger;
        this.commitStrategy = commitStrategy;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.atInfo().log("Assigned partitions {}", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.atInfo().log("Revoked partitions {}", partitions);
        if (consumerShuttingDown.get()) {
            logger.atInfo().log("Consumer is shutting down, skipping commit on revoked partitions");
        } else {
            commitSync();
        }

        clearOffsets(partitions);
        logger.atInfo().log("Cleared offsets for revoked partitions");
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.atInfo().log("Lost partitions {}", partitions);
        clearOffsets(partitions);
        logger.atDebug().log("Cleared offsets");
    }

    private void clearOffsets(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsets.remove(partition);
        }
    }

    @Override
    public void onConsumerShutdown() {
        consumerShuttingDown.set(true);
        commitSync();
    }

    /** Commits all tracked offsets synchronously. Exceptions are logged and suppressed. */
    void commitSync() {
        logger.atInfo().log("Start committing offsets synchronously");
        try {
            Map<TopicPartition, OffsetAndMetadata> snapshot = offsetsSnapshot();
            logger.atInfo().log("Offsets to commit: {}", snapshot);
            consumer.commitSync(snapshot);
            logger.atInfo().log("Offsets committed synchronously");
        } catch (RuntimeException e) {
            logger.atWarn().log("Unable to commit offsets but safe to ignore: {}", e.getMessage());
        }
    }

    @Override
    public void maybeCommit() {
        messagesSinceLastCommit += recordsCounter.getAndSet(0);
        long now = System.currentTimeMillis();
        if (!commitStrategy.canCommit(now, lastCommitTimeMs, messagesSinceLastCommit)) {
            logger.atDebug().log("Skipping commit of {} messages", messagesSinceLastCommit);
            return;
        }

        commitAsync(now);
    }

    void commitAsync(long now) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsetsSnapshot();
        logger.atDebug().log(
                "Start committing of {} messages asynchronously: {}",
                messagesSinceLastCommit,
                offsetsToCommit);

        lastCommitTimeMs = now;
        messagesSinceLastCommit = 0;

        consumer.commitAsync(
                offsetsToCommit,
                (offsets, exception) -> {
                    if (exception == null) {
                        consecutiveCommitFailures.set(0);
                        logger.atDebug().log("Offsets committed asynchronously {}", offsets);
                        return;
                    }
                    int fails = consecutiveCommitFailures.incrementAndGet();
                    logger.atWarn()
                            .setCause(exception)
                            .log(
                                    "Failed to commit offset asynchronously (failurecount={}): {}",
                                    fails);
                });
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
        return Map.copyOf(offsets);
    }

    @Override
    public void updateOffsets(KafkaRecord<?, ?> record) {
        recordsCounter.incrementAndGet();
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        offsets.compute(
                topicPartition,
                (partition, offsetAndMetadata) -> {
                    if (offsetAndMetadata != null && offsetAndMetadata.offset() > record.offset()) {
                        return offsetAndMetadata;
                    }
                    return new OffsetAndMetadata(record.offset() + 1);
                });
    }

    @Override
    public void onAsyncFailure(Throwable th) {
        if (firstFailure == null) {
            firstFailure = th;
        }
    }

    @Override
    public Throwable getFirstFailure() {
        return firstFailure;
    }
}
