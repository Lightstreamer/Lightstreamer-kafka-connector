
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Offsets {

    public static OffsetService OffsetService(Consumer<?, ?> consumer, Logger log) {
        return new OffsetServiceImpl(consumer, log);
    }

    public static OffsetStore OffsetStore(Map<TopicPartition, OffsetAndMetadata> committed) {
        return new OffsetStoreImpl(committed, LoggerFactory.getLogger(OffsetStore.class));
    }

    public interface OffsetStore {

        void save(KafkaRecord<?, ?> record);

        default void clear(Collection<TopicPartition> partitions) {}

        default Map<TopicPartition, OffsetAndMetadata> snapshot() {
            return Collections.emptyMap();
        }
    }

    public interface CommitStrategy {

        boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit);

        static CommitStrategy fixedCommitStrategy(long commitIntervalMs, int commitEveryNRecords) {
            return new FixedThresholdCommitStrategy(commitIntervalMs, commitEveryNRecords);
        }

        static CommitStrategy adaptiveCommitStrategy(int maxCommitsPerSecond) {
            return new AdaptiveThresholdCommitStrategy(maxCommitsPerSecond);
        }
    }

    public static class FixedThresholdCommitStrategy implements CommitStrategy {

        private final long commitIntervalMs;
        private final int commitEveryNRecords;

        FixedThresholdCommitStrategy(long commitIntervalMs, int commitEveryNRecords) {
            // Allow custom thresholds if needed
            this.commitIntervalMs = commitIntervalMs;
            this.commitEveryNRecords = commitEveryNRecords;
        }

        @Override
        public boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit) {
            if (messagesSinceLastCommit == 0) {
                return false;
            }
            boolean byTime = now - lastCommitTimeMs >= this.commitIntervalMs;
            boolean byCount = messagesSinceLastCommit >= this.commitEveryNRecords;

            return byTime || byCount;
        }
    }

    /**
     * Adaptive commit strategy that dynamically adjusts commit frequency based on message rate.
     *
     * <p>Behavior:
     *
     * <ul>
     *   <li>Commits when lag reaches the current message rate (max 1 second worth)
     *   <li>At low throughput: commits more frequently (up to maxCommitsPerSecond)
     *   <li>At high throughput: gradually throttles down to 1 commit/sec
     *   <li>Never waits longer than 10 seconds between commits (safety)
     * </ul>
     *
     * <p>Example with maxCommitsPerSecond=5:
     *
     * <pre>
     * | Message Rate | Effective Commits/sec | Max Lag   | Interval |
     * |--------------|-----------------------|-----------|----------|
     * | 10K msg/sec  | 5.0                   | 2,000     | 200ms    |
     * | 50K msg/sec  | 4.7                   | 10,638    | 213ms    |
     * | 100K msg/sec | 4.3                   | 23,256    | 233ms    |
     * | 200K msg/sec | 3.4                   | 58,824    | 294ms    |
     * | 300K msg/sec | 2.6                   | 115,385   | 385ms    |
     * | 400K msg/sec | 1.8                   | 222,222   | 556ms    |
     * | 500K msg/sec | 1.0                   | 500,000   | 1,000ms  |
     * | 1M msg/sec   | 1.0                   | 1,000,000 | 1,000ms  |
     * </pre>
     */
    private static class AdaptiveThresholdCommitStrategy implements CommitStrategy {

        private final int maxCommitsPerSecond;
        private final long minCommitIntervalMs;
        private final long maxCommitIntervalMs = 10_000L;

        private volatile double avgMessageRate = 0.0;
        private static final double EMA_ALPHA = 0.3;

        // Rate thresholds for scaling commits/sec
        private static final double LOW_RATE = 10_000; // Below: use maxCommitsPerSecond
        private static final double HIGH_RATE = 500_000; // Above: use 1 commit/sec

        AdaptiveThresholdCommitStrategy(int maxCommitsPerSecond) {
            this.maxCommitsPerSecond = maxCommitsPerSecond;
            this.minCommitIntervalMs = 1000L / maxCommitsPerSecond;
        }

        @Override
        public boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit) {
            if (messagesSinceLastCommit == 0) {
                return false;
            }

            long elapsed = now - lastCommitTimeMs;

            // Hard limit: never exceed maxCommitsPerSecond
            if (elapsed < minCommitIntervalMs) {
                return false;
            }

            // Calculate and update average message rate
            double currentRate = elapsed > 0 ? (messagesSinceLastCommit * 1000.0) / elapsed : 0;
            avgMessageRate =
                    avgMessageRate == 0.0
                            ? currentRate
                            : EMA_ALPHA * currentRate + (1 - EMA_ALPHA) * avgMessageRate;

            // Calculate effective commits/sec that scales with message rate:
            // - At LOW_RATE or below: use maxCommitsPerSecond
            // - At HIGH_RATE or above: use 1 commit/sec
            // - In between: linear interpolation
            double effectiveCommitsPerSec;
            if (avgMessageRate <= LOW_RATE) {
                effectiveCommitsPerSec = maxCommitsPerSecond;
            } else if (avgMessageRate >= HIGH_RATE) {
                effectiveCommitsPerSec = 1.0;
            } else {
                // Linear interpolation between maxCommitsPerSecond and 1
                double scale = (avgMessageRate - LOW_RATE) / (HIGH_RATE - LOW_RATE);
                effectiveCommitsPerSec = maxCommitsPerSecond - scale * (maxCommitsPerSecond - 1);
            }

            // Lag threshold = rate / effectiveCommitsPerSec
            int maxLag = Math.max(1, (int) (avgMessageRate / effectiveCommitsPerSec));

            // Commit when lag exceeds threshold OR max interval reached (safety)
            return messagesSinceLastCommit >= maxLag || elapsed >= maxCommitIntervalMs;
        }
    }

    public interface OffsetService extends ConsumerRebalanceListener {

        @FunctionalInterface
        interface OffsetStoreSupplier {

            OffsetStore newOffsetStore(
                    Map<TopicPartition, OffsetAndMetadata> offsets, Logger logger);
        }

        default void initStore(boolean fromLatest) throws KafkaException {
            initStore(fromLatest, Offsets.OffsetStoreImpl::new);
        }

        void initStore(boolean fromLatest, OffsetStoreSupplier storeSupplier) throws KafkaException;

        default void initStore(
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            initStore(Offsets.OffsetStoreImpl::new, startOffsets, committed);
        }

        void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed);

        void maybeCommit();

        void updateOffsets(KafkaRecord<?, ?> record);

        void onAsyncFailure(Throwable th);

        void onConsumerShutdown();

        Throwable getFirstFailure();

        Optional<OffsetStore> offsetStore();
    }

    static class OffsetServiceImpl implements OffsetService {

        private volatile Throwable firstFailure;
        private final AtomicBoolean consumerShuttingDown = new AtomicBoolean(false);
        private final Consumer<?, ?> consumer;
        private final Logger logger;
        private final AtomicInteger recordsCounter = new AtomicInteger();
        private final CommitStrategy commitStrategy;

        // Initialize the OffsetStore with a NOP implementation
        private OffsetStore offsetStore = record -> {};

        OffsetServiceImpl(Consumer<?, ?> consumer, Logger logger) {
            this(consumer, logger, CommitStrategy.adaptiveCommitStrategy(5));
        }

        OffsetServiceImpl(Consumer<?, ?> consumer, Logger logger, CommitStrategy commitStrategy) {
            this.consumer = consumer;
            this.logger = logger;
            this.commitStrategy = commitStrategy;
        }

        @Override
        public void initStore(boolean fromLatest, OffsetStoreSupplier storeSupplier)
                throws KafkaException {
            Set<TopicPartition> partitions = this.consumer.assignment();
            // Retrieve the offset to start from, which has to be used in case no
            // committed offset is available for a given partition.
            // The start offset depends on the auto.offset.reset property.
            Map<TopicPartition, Long> startOffsets =
                    fromLatest
                            ? this.consumer.endOffsets(partitions)
                            : this.consumer.beginningOffsets(partitions);
            // Get the current committed offsets for all the assigned partitions
            Map<TopicPartition, OffsetAndMetadata> committed = this.consumer.committed(partitions);
            initStore(storeSupplier, startOffsets, committed);
        }

        @Override
        public void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            Map<TopicPartition, OffsetAndMetadata> offsetRepo = new HashMap<>(committed);
            logger.atTrace().log("Start offsets: {}", startOffsets);
            logger.atTrace().log("Committed offsets map: {}", offsetRepo);
            // If a partition misses a committed offset for a partition, just put the
            // the current offset.
            for (TopicPartition partition : startOffsets.keySet()) {
                offsetRepo.computeIfAbsent(
                        partition, p -> new OffsetAndMetadata(startOffsets.get(p)));
            }
            this.offsetStore = storeSupplier.newOffsetStore(offsetRepo, logger);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.atInfo().log("Assigned partitions {}", partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.atInfo().log("Revoked partitions {}", partitions);
            if (consumerShuttingDown.get()) {
                logger.atInfo().log(
                        "Consumer is shutting down, skipping commit on revoked partitions");
            } else {
                commitSync();
            }

            offsetStore.clear(partitions);
            logger.atInfo().log("Cleared offsets for revoked partitions");
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            logger.atInfo().log("Lost partitions {}", partitions);
            offsetStore.clear(partitions);
            logger.atDebug().log("Cleared offsets");
        }

        @Override
        public void onConsumerShutdown() {
            consumerShuttingDown.set(true);
            commitSync();
        }

        void commitSync() {
            logger.atInfo().log("Start committing offsets synchronously");
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets = offsetStore.snapshot();
                logger.atInfo().log("Offsets to commit: {}", offsets);
                consumer.commitSync(offsets);
                logger.atInfo().log("Offsets committed synchronously");
            } catch (RuntimeException e) {
                logger.atWarn()
                        .log("Unable to commit offsets but safe to ignore: {}", e.getMessage());
            }
        }

        private volatile long lastCommitTimeMs = System.currentTimeMillis();
        private volatile int messagesSinceLastCommit = 0;
        private final AtomicInteger consecutiveCommitFailures = new AtomicInteger(0);

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
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsetStore.snapshot();
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
        public void updateOffsets(KafkaRecord<?, ?> record) {
            recordsCounter.incrementAndGet();
            offsetStore.save(record);
        }

        @Override
        public void onAsyncFailure(Throwable th) {
            if (firstFailure == null) {
                firstFailure = th; // any of the first exceptions got should be enough
            }
        }

        public Throwable getFirstFailure() {
            return firstFailure;
        }

        @Override
        public Optional<OffsetStore> offsetStore() {
            return Optional.of(offsetStore);
        }
    }

    private static class OffsetStoreImpl implements OffsetStore {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Logger logger;

        OffsetStoreImpl(Map<TopicPartition, OffsetAndMetadata> committed, Logger logger) {
            this.offsets = new ConcurrentHashMap<>(committed);
            this.logger = logger;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> snapshot() {
            return new HashMap<>(offsets);
        }

        @Override
        public void save(KafkaRecord<?, ?> record) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            offsets.compute(
                    topicPartition,
                    (partition, offsetAndMetadata) -> {
                        if (offsetAndMetadata != null
                                && offsetAndMetadata.offset() > record.offset()) {
                            return offsetAndMetadata;
                        }
                        return new OffsetAndMetadata(record.offset() + 1);
                    });
        }

        @Override
        public void clear(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                offsets.remove(partition);
            }
        }
    }

    private Offsets() {}
}
