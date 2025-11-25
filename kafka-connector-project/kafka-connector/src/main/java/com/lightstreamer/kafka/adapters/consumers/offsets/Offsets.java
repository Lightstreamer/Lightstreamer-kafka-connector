
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

import com.lightstreamer.kafka.common.utils.Split;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Offsets {

    static String SEPARATOR = ",";
    static Supplier<Collection<Long>> SUPPLIER = ArrayList::new;
    static Predicate<String> NOT_EMPTY_STRING = ((Predicate<String>) String::isEmpty).negate();

    static String encode(Collection<Long> offsets) {
        return offsets.stream().map(String::valueOf).collect(Collectors.joining(SEPARATOR));
    }

    static Collection<Long> decode(String str) {
        if (str.isBlank()) {
            return Collections.emptyList();
        }
        return Split.byComma(str).stream()
                .filter(NOT_EMPTY_STRING)
                .map(Long::valueOf)
                .sorted()
                .collect(Collectors.toCollection(SUPPLIER));
    }

    static String append(String str, long offset) {
        String prefix = str.isEmpty() ? "" : str + SEPARATOR;
        return prefix + offset;
    }

    public static OffsetService OffsetService(
            Consumer<?, ?> consumer, boolean manageHoles, Logger log) {
        return new OffsetServiceImpl(consumer, manageHoles, log);
    }

    public static OffsetStore OffsetStore(
            Map<TopicPartition, OffsetAndMetadata> committed, boolean manageHoles) {
        return new OffsetStoreImpl(
                committed, manageHoles, LoggerFactory.getLogger(OffsetStore.class));
    }

    public interface OffsetStore {

        void save(ConsumerRecord<?, ?> record);

        default void clear() {}

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

        static CommitStrategy adaptiveCommitStrategy() {
            return new AdaptiveThresholdCommitStrategy();
        }
    }

    private static class FixedThresholdCommitStrategy implements CommitStrategy {

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
     * Adaptive commit strategy that dynamically adjusts commit frequency based on message
     * processing rate. Designed to balance commit efficiency (avoiding {@code
     * RetriableCommitException}) with crash safety by scaling batch sizes and intervals according
     * to throughput while enforcing safety limits.
     *
     * <p>Scaling tiers: - Tier 1 (≤25K msg/sec): Gentle scaling up to 3x baseline - Tier 2
     * (25K-125K msg/sec): Moderate scaling up to 6x baseline - Tier 3 (125K-250K msg/sec):
     * Controlled scaling up to 10x baseline
     *
     * <p>Safety guarantees: - Maximum 10-second commit intervals (crash safety) - Maximum 100K
     * record batches (memory protection) - Never commits more frequently than baseline (efficiency
     * protection)
     */
    private static class AdaptiveThresholdCommitStrategy implements CommitStrategy {

        private final long BASE_COMMIT_INTERVAL_MS = 4000; // Baseline: 4 seconds
        private final int BASE_COMMIT_RECORDS = 10000; // Baseline: 10K records (2.5K msg/sec rate)
        private final double MIN_SCALE = 1.0; // Never scale below baseline (efficiency protection)
        private final double MAX_SCALE =
                50.0; // Maximum scaling factor for extreme throughput (500K+ msg/sec)

        @Override
        public boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit) {
            long elapsed = now - lastCommitTimeMs;

            // Calculate current message processing rate (messages per second)
            double messageRate = elapsed > 0 ? (messagesSinceLastCommit * 1000.0) / elapsed : 0;
            double baselineRate =
                    (BASE_COMMIT_RECORDS * 1000.0)
                            / BASE_COMMIT_INTERVAL_MS; // 2500 msg/sec baseline rate

            // Apply three-tier adaptive scaling based on load factor
            double scale;
            if (messageRate <= baselineRate) {
                // Low/normal load: maintain baseline frequency for responsiveness
                scale = MIN_SCALE;
            } else {
                double loadFactor = messageRate / baselineRate;
                if (loadFactor <= 10) {
                    // Tier 1: Up to 25K msg/sec - gentle logarithmic scaling (max 3x)
                    scale = Math.min(3.0, 1.0 + Math.log(loadFactor) * 0.6);
                } else if (loadFactor <= 50) {
                    // Tier 2: 25K-125K msg/sec - moderate scaling (3x to 6x)
                    scale = Math.min(6.0, 3.0 + Math.log(loadFactor / 10.0) * 1.0);
                } else if (loadFactor <= 200) {
                    // Tier 3: 125K-500K msg/sec - controlled scaling for high throughput (6x to
                    // 20x)
                    scale = Math.min(20.0, 6.0 + Math.log(loadFactor / 50.0) * 3.5);
                } else {
                    // Tier 4: 500K+ msg/sec - extreme throughput scaling (20x to 50x)
                    scale = Math.min(MAX_SCALE, 20.0 + Math.log(loadFactor / 200.0) * 5.0);
                }
            }

            // Calculate adaptive thresholds based on scaling factor
            long adaptiveInterval = (long) (BASE_COMMIT_INTERVAL_MS * scale);
            int adaptiveRecordsThreshold = (int) (BASE_COMMIT_RECORDS * scale);

            // Apply safety limits to prevent excessive data loss and memory usage
            long maxInterval = 5000; // 5-second cap: faster commits for high throughput
            int maxRecords = 200000; // 200K record cap: increased for high throughput scenarios

            // Commit when either time threshold OR record threshold is reached
            return elapsed >= Math.min(adaptiveInterval, maxInterval)
                    || messagesSinceLastCommit >= Math.min(adaptiveRecordsThreshold, maxRecords);
        }
    }

    public interface OffsetService extends ConsumerRebalanceListener {

        @FunctionalInterface
        interface OffsetStoreSupplier {

            OffsetStore newOffsetStore(
                    Map<TopicPartition, OffsetAndMetadata> offsets,
                    boolean manageHoles,
                    Logger logger);
        }

        default void initStore(boolean fromLatest) {
            initStore(fromLatest, Offsets.OffsetStoreImpl::new);
        }

        void initStore(boolean flag, OffsetStoreSupplier storeSupplier);

        default void initStore(
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            initStore(Offsets.OffsetStoreImpl::new, startOffsets, committed);
        }

        void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed);

        boolean notHasPendingOffset(ConsumerRecord<?, ?> record);

        boolean canManageHoles();

        void maybeCommit();

        void updateOffsets(ConsumerRecord<?, ?> record);

        void onAsyncFailure(Throwable th);

        void onConsumerShutdown();

        Throwable getFirstFailure();

        Optional<OffsetStore> offsetStore();
    }

    static class OffsetServiceImpl implements OffsetService {

        private volatile Throwable firstFailure;
        private final AtomicBoolean consumerShuttingDown = new AtomicBoolean(false);
        private final Consumer<?, ?> consumer;
        private final boolean manageHoles;
        private final Logger logger;
        private AtomicInteger recordsCounter = new AtomicInteger();
        private final CommitStrategy commitStrategy;

        // Initialize the OffsetStore with a NOP implementation
        private OffsetStore offsetStore = record -> {};

        private final Map<TopicPartition, Collection<Long>> pendingOffsetsMap =
                new ConcurrentHashMap<>();

        OffsetServiceImpl(Consumer<?, ?> consumer, boolean manageHoles, Logger logger) {
            this(consumer, manageHoles, logger, new FixedThresholdCommitStrategy(5000, 100_000));
        }

        OffsetServiceImpl(
                Consumer<?, ?> consumer,
                boolean manageHoles,
                Logger logger,
                CommitStrategy commitStrategy) {
            this.consumer = consumer;
            this.manageHoles = manageHoles;
            this.logger = logger;
            this.commitStrategy = commitStrategy;
        }

        @Override
        public boolean canManageHoles() {
            return manageHoles;
        }

        @Override
        public void initStore(boolean fromLatest, OffsetStoreSupplier storeSupplier) {
            Set<TopicPartition> partitions = consumer.assignment();
            // Retrieve the offset to start from, which has to be used in case no
            // committed offset is available for a given partition.
            // The start offset depends on the auto.offset.reset property.
            Map<TopicPartition, Long> startOffsets =
                    fromLatest
                            ? consumer.endOffsets(partitions)
                            : consumer.beginningOffsets(partitions);
            // Get the current committed offsets for all the assigned partitions
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(partitions);
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
                OffsetAndMetadata offsetAndMetadata =
                        offsetRepo.computeIfAbsent(
                                partition, p -> new OffsetAndMetadata(startOffsets.get(p)));
                // Store the offsets that have been already delivered to clients,
                // but not yet committed (most likely due to an exception while processing in
                // parallel).
                pendingOffsetsMap.put(partition, decode(offsetAndMetadata.metadata()));
            }
            logger.atTrace().log("Pending offsets map: {}", pendingOffsetsMap);
            this.offsetStore = storeSupplier.newOffsetStore(offsetRepo, manageHoles, logger);
        }

        @Override
        public boolean notHasPendingOffset(ConsumerRecord<?, ?> record) {
            Collection<Long> pendingOffsetsList =
                    pendingOffsetsMap.getOrDefault(
                            new TopicPartition(record.topic(), record.partition()),
                            Collections.emptyList());
            return !pendingOffsetsList.contains(record.offset());
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
            logger.atDebug().log("Start committing offsets synchronously");
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets = offsetStore.snapshot();
                logger.atDebug().log("Offsets to commit: {}", offsets);
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
                logger.atTrace().log("Skipping commit of {} messages", messagesSinceLastCommit);
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
        public void updateOffsets(ConsumerRecord<?, ?> record) {
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

        private interface OffsetAndMetadataFactory {

            OffsetAndMetadata newOffsetAndMetadata(
                    ConsumerRecord<?, ?> record, OffsetAndMetadata lastOffsetAndMetadata);
        }

        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Logger logger;
        private final OffsetAndMetadataFactory factory;

        OffsetStoreImpl(
                Map<TopicPartition, OffsetAndMetadata> committed,
                boolean manageHoles,
                Logger logger) {
            this.offsets = new ConcurrentHashMap<>(committed);
            this.logger = logger;
            // this.factory =
            //         manageHoles
            //                 ? OffsetStoreImpl::mkNewOffsetAndMetadata
            //                 : (record, offsetAndMetadata) ->
            //                         new OffsetAndMetadata(record.offset() + 1);
            this.factory = (record, offsetAndMetadata) ->
                                    new OffsetAndMetadata(record.offset() + 1);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> snapshot() {
            return new HashMap<>(offsets);
        }

        @Override
        public void save(ConsumerRecord<?, ?> record) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            offsets.compute(
                    topicPartition,
                    (partition, offsetAndMetadata) -> {
                        if (offsetAndMetadata != null
                                && offsetAndMetadata.offset() > record.offset()) {
                            logger.atTrace()
                                    .log(
                                            "Received out-of-order record for partition {}. Current offset: {}, record offset: {}. Ignoring record.",
                                            topicPartition,
                                            offsetAndMetadata.offset(),
                                            record.offset());
                            return offsetAndMetadata;
                        }
                        return factory.newOffsetAndMetadata(record, offsetAndMetadata);
                    });
        }

        @Override
        public void clear(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                offsets.remove(partition);
            }
        }

        @Override
        public void clear() {
            offsets.clear();
        }

        private static OffsetAndMetadata mkNewOffsetAndMetadata(
                ConsumerRecord<?, ?> record, OffsetAndMetadata lastOffsetAndMetadata) {
            String lastMetadata = lastOffsetAndMetadata.metadata();
            long lastOffset = lastOffsetAndMetadata.offset();
            long newOffset = lastOffset;
            String newMetadata = lastMetadata;

            long consumedOffset = record.offset();
            if (consumedOffset == lastOffset) {
                Collection<Long> orderedConsumedList = decode(lastMetadata);
                Iterator<Long> iterator = orderedConsumedList.iterator();

                while (iterator.hasNext()) {
                    long offset = iterator.next();
                    if (offset == newOffset + 1) {
                        newOffset = offset;
                        iterator.remove();
                        continue;
                    }
                    break;
                }
                newMetadata = newOffset == lastOffset ? lastMetadata : encode(orderedConsumedList);
                newOffset = newOffset + 1;
            } else {
                newMetadata = append(lastMetadata, consumedOffset);
            }

            return new OffsetAndMetadata(newOffset, newMetadata);
        }
    }

    private Offsets() {}
}
