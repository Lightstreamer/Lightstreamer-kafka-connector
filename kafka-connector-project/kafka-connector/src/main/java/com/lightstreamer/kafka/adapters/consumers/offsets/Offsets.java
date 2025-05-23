
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
import org.apache.kafka.common.KafkaException;
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

    public static OffsetService OffsetService(Consumer<?, ?> consumer) {
        return new OffsetServiceImpl(consumer, LoggerFactory.getLogger(OffsetService.class));
    }

    public static OffsetService OffsetService(Consumer<?, ?> consumer, Logger log) {
        return new OffsetServiceImpl(consumer, log);
    }

    public static OffsetStore OffsetStore(Map<TopicPartition, OffsetAndMetadata> committed) {
        return new OffsetStoreImpl(committed);
    }

    public interface OffsetStore {

        void save(ConsumerRecord<?, ?> record);

        default Map<TopicPartition, OffsetAndMetadata> current() {
            return Collections.emptyMap();
        }
    }

    public interface OffsetService extends ConsumerRebalanceListener {

        @FunctionalInterface
        interface OffsetStoreSupplier {

            OffsetStore newOffsetStore(Map<TopicPartition, OffsetAndMetadata> offsets);
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

        void commitSync();

        void commitSyncAndIgnoreErrors();

        void commitAsync();

        void updateOffsets(ConsumerRecord<?, ?> record);

        void onAsyncFailure(Throwable th);

        Throwable getFirstFailure();

        Optional<OffsetStore> offsetStore();
    }

    private static class OffsetServiceImpl implements OffsetService {

        private volatile Throwable firstFailure;
        private final Consumer<?, ?> consumer;
        private final Logger log;

        // Initialize the OffsetStore with a NOP implementation
        private OffsetStore offsetStore = record -> {};

        private Map<TopicPartition, Collection<Long>> pendingOffsetsMap = new ConcurrentHashMap<>();

        OffsetServiceImpl(Consumer<?, ?> consumer, Logger logger) {
            this.consumer = consumer;
            this.log = logger;
        }

        @Override
        public void initStore(boolean fromLatest, OffsetStoreSupplier storeSupplier) {
            Set<TopicPartition> partitions = consumer.assignment();
            // Retrieve the offset to start from, which has to be used in case no committed offset
            // is
            // available for a given partition.
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
            offsetStore = storeSupplier.newOffsetStore(offsetRepo);
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
            log.atDebug().log("Assigned partitions {}", partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.atWarn().log("Partitions revoked");
            commitSync();
        }

        @Override
        public void commitSync() {
            commitSync(false);
        }

        @Override
        public void commitSyncAndIgnoreErrors() {
            commitSync(true);
        }

        private void commitSync(boolean ignoreErrors) {
            try {
                log.atDebug().log("Start committing offset synchronously");
                consumer.commitSync(offsetStore.current());
                log.atInfo().log("Offsets committed");
            } catch (KafkaException e) {
                log.atError().setCause(e).log("Unable to commit offsets");
                if (!ignoreErrors) {
                    log.atDebug().log("Rethrowing the error");
                    throw e;
                }
            }
        }

        @Override
        public void commitAsync() {
            consumer.commitAsync(offsetStore.current(), null);
        }

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {
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

        OffsetStoreImpl(Map<TopicPartition, OffsetAndMetadata> committed) {
            this.offsets = new ConcurrentHashMap<>(committed);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> current() {
            return Collections.unmodifiableMap(offsets);
        }

        @Override
        public void save(ConsumerRecord<?, ?> record) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            offsets.compute(topicPartition, (p, O) -> mkNewOffsetAndMetadata(record, O));
        }

        private OffsetAndMetadata mkNewOffsetAndMetadata(
                ConsumerRecord<?, ?> record, OffsetAndMetadata lastOffsetAndMetadata) {
            String lastMetadata = lastOffsetAndMetadata.metadata();
            long lastOffset = lastOffsetAndMetadata.offset();
            long consumedOffset = record.offset();
            if (consumedOffset == lastOffset) {
                Collection<Long> orderedConsumedList = decode(lastMetadata);
                Iterator<Long> iterator = orderedConsumedList.iterator();

                long newOffset = lastOffset;
                while (iterator.hasNext()) {
                    long offset = iterator.next();
                    if (offset == newOffset + 1) {
                        newOffset = offset;
                        iterator.remove();
                        continue;
                    }
                    break;
                }
                String newMetadata =
                        newOffset == lastOffset ? lastMetadata : encode(orderedConsumedList);
                return new OffsetAndMetadata(newOffset + 1, newMetadata);
            }
            String newMetadata = append(lastMetadata, consumedOffset);
            return new OffsetAndMetadata(lastOffset, newMetadata);
        }
    }

    private Offsets() {}
}
