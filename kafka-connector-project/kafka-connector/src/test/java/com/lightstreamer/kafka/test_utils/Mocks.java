
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

package com.lightstreamer.kafka.test_utils;

import com.lightstreamer.interfaces.data.DiffAlgorithm;
import com.lightstreamer.interfaces.data.IndexedItemEvent;
import com.lightstreamer.interfaces.data.ItemEvent;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class Mocks {

    public static class MockConsumer<K, V>
            extends org.apache.kafka.clients.consumer.MockConsumer<K, V> {

        private KafkaException commitException;
        private KafkaException listTopicException;

        public MockConsumer(OffsetResetStrategy offsetResetStrategy) {
            super(offsetResetStrategy);
        }

        public void setCommitException(KafkaException exception) {
            this.commitException = exception;
        }

        public void setListTopicException(Exception exception) {
            this.listTopicException = new KafkaException("Mocked listTopics exception", exception);
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (commitException != null) {
                throw commitException;
            }

            super.commitSync(offsets);
        }

        @Override
        public synchronized Map<String, List<PartitionInfo>> listTopics() {
            if (listTopicException != null) {
                throw listTopicException;
            }
            return super.listTopics();
        }

        public static <K, V> Supplier<Consumer<K, V>> supplier() {
            return supplier(false);
        }

        public static <K, V> Supplier<Consumer<K, V>> supplier(boolean exceptionOnConnection) {
            return () -> {
                if (exceptionOnConnection) {
                    throw new KafkaException("Simulated Exception");
                }
                return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            };
        }

        public static <K, V> Supplier<Consumer<K, V>> supplier(String... topics) {
            return () -> {
                MockConsumer<K, V> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
                for (String topic : topics) {
                    mockConsumer.updatePartitions(
                            topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
                }

                return mockConsumer;
            };
        }
    }

    public static class MockMetadataListener implements MetadataListener {

        private volatile boolean forcedUnsubscription = false;

        @Override
        public void forceUnsubscription(String item) {}

        @Override
        public void forceUnsubscriptionAll() {
            forcedUnsubscription = true;
        }

        public boolean forcedUnsubscription() {
            return forcedUnsubscription;
        }
    }

    public static class MockOffsetService implements OffsetService {

        public static record ConsumedRecordInfo(String topic, int partition, Long offset) {
            static ConsumedRecordInfo from(ConsumerRecord<?, ?> record) {
                return new ConsumedRecordInfo(record.topic(), record.partition(), record.offset());
            }
        }

        private final List<ConsumedRecordInfo> records =
                Collections.synchronizedList(new ArrayList<>());
        private volatile Throwable firstFailure;

        public MockOffsetService() {}

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

        @Override
        public void commitSync() {}

        @Override
        public void commitSyncAndIgnoreErrors() {}

        @Override
        public void commitAsync() {}

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {
            records.add(
                    new ConsumedRecordInfo(record.topic(), record.partition(), record.offset()));
        }

        @Override
        public void onAsyncFailure(Throwable th) {
            if (firstFailure == null) {
                firstFailure = th; // any of the first exceptions got should be enough
            }
        }

        @Override
        public Throwable getFirstFailure() {
            return firstFailure;
        }

        @Override
        public void initStore(boolean flag, OffsetStoreSupplier storeSupplier) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }

        @Override
        public boolean notHasPendingOffset(ConsumerRecord<?, ?> record) {
            throw new UnsupportedOperationException("Unimplemented method 'isAlreadyConsumed'");
        }

        @Override
        public Optional<OffsetStore> offsetStore() {
            throw new UnsupportedOperationException("Unimplemented method 'offsetStore'");
        }

        @Override
        public void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }

        public List<ConsumedRecordInfo> getConsumedRecords() {
            return records;
        }

        @Override
        public boolean canManageHoles() {
            return false;
        }
    }

    public static class MockOffsetStore implements OffsetStore {

        private final List<ConsumerRecord<?, ?>> records = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> topicMap;

        public MockOffsetStore(
                Map<TopicPartition, OffsetAndMetadata> topicMap, boolean parallel, Logger log) {
            this.topicMap = Collections.unmodifiableMap(topicMap);
        }

        @Override
        public void save(ConsumerRecord<?, ?> record) {
            records.add(record);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> current() {
            return topicMap;
        }

        public List<ConsumerRecord<?, ?>> getRecords() {
            return Collections.unmodifiableList(records);
        }
    }

    public static class MockRecordProcessor<K, V> implements RecordProcessor<K, V> {

        private List<ConsumedRecordInfo> offsetTriggeringExceptions;
        private RuntimeException e;
        private boolean allowConcurrentProcessing;

        public MockRecordProcessor(
                RuntimeException e,
                List<ConsumedRecordInfo> offsetTriggeringExceptions,
                boolean allowConcurrentProcessing) {
            this.e = e;
            this.offsetTriggeringExceptions = offsetTriggeringExceptions;
            this.allowConcurrentProcessing = allowConcurrentProcessing;
        }

        public MockRecordProcessor(
                RuntimeException e, List<ConsumedRecordInfo> offsetTriggeringExceptions) {
            this(e, offsetTriggeringExceptions, true);
        }

        public MockRecordProcessor(boolean allowConcurrentProcessing) {
            this(null, Collections.emptyList(), allowConcurrentProcessing);
        }

        public MockRecordProcessor() {
            this(null, Collections.emptyList(), true);
        }

        @Override
        public void process(ConsumerRecord<K, V> record) throws ValueException {
            if (e == null) {
                return;
            }

            if (offsetTriggeringExceptions.contains(ConsumedRecordInfo.from(record))) {
                throw e;
            }
        }

        @Override
        public void useLogger(Logger logger) {}

        @Override
        public ProcessUpdatesType processUpdatesType() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'processUpdatesType'");
        }
    }

    public static class MockItemEventListener implements ItemEventListener {

        public static BiConsumer<Map<String, String>, Boolean> NOPConsumer = (m, s) -> {};

        private final BiConsumer<Map<String, String>, Boolean> consumer;
        private final BiConsumer<Map<String, String>, Boolean> legacyConsumer;

        boolean smartClearSnapshotCalled = false;
        boolean smartEndOfSnapshotCalled = false;

        boolean legacyClearSnapshotCalled = false;
        boolean legacyEndOfSnapshotCalled = false;

        public MockItemEventListener(BiConsumer<Map<String, String>, Boolean> smartConsumer) {
            this(smartConsumer, NOPConsumer);
        }

        public MockItemEventListener(
                BiConsumer<Map<String, String>, Boolean> smartConsumer,
                BiConsumer<Map<String, String>, Boolean> legacyConsumer) {
            this.consumer = smartConsumer;
            this.legacyConsumer = legacyConsumer;
        }

        public MockItemEventListener() {
            this(NOPConsumer, NOPConsumer);
        }

        public boolean smartClearSnapshotCalled() {
            return smartClearSnapshotCalled;
        }

        public boolean legacyEndOfSnapshotCalled() {
            return legacyEndOfSnapshotCalled;
        }

        public boolean smartEndOfSnapshotCalled() {
            return smartEndOfSnapshotCalled;
        }

        public boolean legacyClearSnapshotCalled() {
            return legacyClearSnapshotCalled;
        }

        @Override
        public void update(String itemName, ItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, Map event, boolean isSnapshot) {
            legacyConsumer.accept(event, isSnapshot);
        }

        @Override
        public void update(String itemName, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void smartUpdate(Object itemHandle, ItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, Map event, boolean isSnapshot) {
            consumer.accept(event, isSnapshot);
        }

        @Override
        public void smartUpdate(Object itemHandle, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            this.legacyEndOfSnapshotCalled = true;
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            this.smartEndOfSnapshotCalled = true;
        }

        @Override
        public void clearSnapshot(String itemName) {
            this.legacyClearSnapshotCalled = true;
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            this.smartClearSnapshotCalled = true;
        }

        @Override
        public void declareFieldDiffOrder(
                String itemName, Map<String, DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException("Unimplemented method 'declareFieldDiffOrder'");
        }

        @Override
        public void smartDeclareFieldDiffOrder(
                Object itemHandle, Map<String, DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'smartDeclareFieldDiffOrder'");
        }

        @Override
        public void failure(Throwable e) {
            throw new UnsupportedOperationException("Unimplemented method 'failure'");
        }
    }
}
