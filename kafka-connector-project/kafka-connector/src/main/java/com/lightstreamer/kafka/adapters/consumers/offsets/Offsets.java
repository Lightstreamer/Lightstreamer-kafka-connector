
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

import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class Offsets {

    static class OffsetServiceImpl implements OffsetService {

        private volatile ValueException firstFailure;
        private final Consumer<?, ?> consumer;
        private final Logger log;

        // Initialize the OffsetStore with a NOP implementation
        private OffsetStore offsetStore = record -> {};

        private Map<TopicPartition, Collection<Long>> skipMap = new ConcurrentHashMap<>();

        OffsetServiceImpl(Consumer<?, ?> consumer, Logger logger) {
            this.consumer = consumer;
            this.log = logger;
        }

        OffsetServiceImpl(Consumer<?, ?> consumer) {
            this(consumer, LoggerFactory.getLogger(OffsetService.class));
        }

        @Override
        public void initStore(boolean fromLatest, OffsetStoreSupplier storeSupplier) {
            Set<TopicPartition> partitions = consumer.assignment();
            // Retrieve the offset to start from, which has to be used in case no commited offset is
            // available fora given partition.
            // The start offset depemds on the auto.offset.reset property.
            Map<TopicPartition, Long> startOffsets =
                    fromLatest
                            ? consumer.endOffsets(partitions)
                            : consumer.beginningOffsets(partitions);

            // Get the current commited offsets for all the assigned partitions
            Map<TopicPartition, OffsetAndMetadata> committed =
                    new HashMap<>(consumer.committed(partitions));
            // In case of missing the commited offset for a parition, just put the the current
            // offset
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata offsetAndMetadata =
                        committed.computeIfAbsent(
                                partition, p -> new OffsetAndMetadata(startOffsets.get(p)));
                Collection<Long> alreadyConsumedOffset =
                        OffsetStore.decode(offsetAndMetadata.metadata());
                if (!alreadyConsumedOffset.isEmpty()) {
                    skipMap.put(partition, alreadyConsumedOffset);
                }
            }
            offsetStore = storeSupplier.newOffsetStore(committed);
        }

        @Override
        public boolean isNotAlreadyConsumed(ConsumerRecord<?, ?> record) {
            Collection<Long> alreadyConsumedOffsets =
                    skipMap.getOrDefault(
                            new TopicPartition(record.topic(), record.partition()),
                            Collections.emptyList());
            return !alreadyConsumedOffsets.contains(record.offset());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.atDebug().log("Assigned partiions {}", partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.atWarn().log("Partions revoked");
            commitSync();
        }

        @Override
        public void commitSync() {
            consumer.commitSync(offsetStore.getStore());
            log.atInfo().log("Offsets commited");
        }

        @Override
        public void commitAsync() {
            consumer.commitAsync(offsetStore.getStore(), null);
        }

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {
            offsetStore.save(record);
        }

        @Override
        public void onAsyncFailure(ValueException ve) {
            if (firstFailure == null) {
                firstFailure = ve; // any of the first exceptions got should be enough
            }
        }

        public ValueException getFirstFailure() {
            return firstFailure;
        }
    }

    static class OffsetStoreImpl implements OffsetStore {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        OffsetStoreImpl(Map<TopicPartition, OffsetAndMetadata> committed) {
            this.offsets = new HashMap<>(committed);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> getStore() {
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
                Collection<Long> orderedConsumedList = OffsetStore.decode(lastMetadata);
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
                        newOffset == lastOffset
                                ? lastMetadata
                                : OffsetStore.encode(orderedConsumedList);
                return new OffsetAndMetadata(newOffset + 1, newMetadata);
            }
            String newMetadata = OffsetStore.append(lastMetadata, consumedOffset);
            return new OffsetAndMetadata(lastOffset, newMetadata);
        }
    }
}
