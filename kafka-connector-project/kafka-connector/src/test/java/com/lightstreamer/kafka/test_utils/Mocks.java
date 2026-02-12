
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

import static org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType.EARLIEST;

import com.lightstreamer.interfaces.data.DiffAlgorithm;
import com.lightstreamer.interfaces.data.IndexedItemEvent;
import com.lightstreamer.interfaces.data.ItemEvent;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.monitors.Observer;
import com.lightstreamer.kafka.common.monitors.metrics.Meter;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class Mocks {

    public static class MockConsumer
            extends org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]> {

        private RuntimeException commitException;
        private KafkaException listTopicException;

        public MockConsumer(String strategyType) {
            super(strategyType);
        }

        public void setCommitException(RuntimeException exception) {
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

        public static Supplier<Consumer<byte[], byte[]>> supplier() {
            return supplier(false);
        }

        public static Supplier<Consumer<byte[], byte[]>> supplier(boolean exceptionOnConnection) {
            return () -> {
                if (exceptionOnConnection) {
                    throw new KafkaException("Simulated Exception");
                }
                return new MockConsumer(EARLIEST.toString());
            };
        }

        public static Supplier<Consumer<byte[], byte[]>> supplier(String... topics) {
            return () -> {
                MockConsumer mockConsumer = new MockConsumer(EARLIEST.toString());
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
            static ConsumedRecordInfo from(KafkaRecord<?, ?> record) {
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
        public void maybeCommit() {}

        @Override
        public void updateOffsets(KafkaRecord<?, ?> record) {
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
        public void onConsumerShutdown() {}
    }

    public static class MockOffsetStore implements OffsetStore {

        private final List<KafkaRecord<?, ?>> records = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> topicMap;

        public MockOffsetStore(Map<TopicPartition, OffsetAndMetadata> topicMap, Logger log) {
            this.topicMap = Collections.unmodifiableMap(topicMap);
        }

        @Override
        public void save(KafkaRecord<?, ?> record) {
            records.add(record);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> snapshot() {
            return topicMap;
        }

        public List<KafkaRecord<?, ?>> getRecords() {
            return Collections.unmodifiableList(records);
        }
    }

    public static class MockRecordProcessor<K, V> implements RecordProcessor<K, V> {

        private List<ConsumedRecordInfo> offsetTriggeringExceptions;
        private RuntimeException e;
        private ProcessUpdatesType processUpdatesType;

        public MockRecordProcessor(
                RuntimeException e,
                List<ConsumedRecordInfo> offsetTriggeringExceptions,
                ProcessUpdatesType processUpdatesType) {
            this.e = e;
            this.offsetTriggeringExceptions = offsetTriggeringExceptions;
            this.processUpdatesType = processUpdatesType;
        }

        public MockRecordProcessor(
                RuntimeException e, List<ConsumedRecordInfo> offsetTriggeringExceptions) {
            this(e, offsetTriggeringExceptions, ProcessUpdatesType.DEFAULT);
        }

        public MockRecordProcessor(ProcessUpdatesType processUpdatesType) {
            this(null, Collections.emptyList(), processUpdatesType);
        }

        public MockRecordProcessor() {
            this(null, Collections.emptyList(), ProcessUpdatesType.DEFAULT);
        }

        @Override
        public void process(KafkaRecord<K, V> record) throws ValueException {
            if (e == null) {
                return;
            }

            if (offsetTriggeringExceptions.contains(ConsumedRecordInfo.from(record))) {
                throw e;
            }
        }

        @Override
        public void processAsSnapshot(KafkaRecord<K, V> record, SubscribedItem subscribedItem)
                throws ValueException {
            throw new UnsupportedOperationException("Unimplemented method 'processAsSnapshot'");
        }

        @Override
        public void useLogger(Logger logger) {}

        @Override
        public ProcessUpdatesType processUpdatesType() {
            return this.processUpdatesType;
        }
    }

    public static record UpdateCall(Object handle, Map<String, String> event, boolean isSnapshot) {}

    /** Test double for ItemEventListener that records all method calls for verification */
    public static class MockItemEventListener implements ItemEventListener {

        private final List<UpdateCall> smartSnapshotUpdates =
                Collections.synchronizedList(new ArrayList<>());
        private final List<UpdateCall> snapshotUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<UpdateCall> smartRealtimeUpdates =
                Collections.synchronizedList(new ArrayList<>());
        private final List<UpdateCall> realtimeUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<UpdateCall> allUpdatesChronological =
                Collections.synchronizedList(new ArrayList<>());

        private final List<Object> smartClearSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());
        private final List<String> clearSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());

        private final List<Object> smartEndOfSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());
        private final List<String> endOfSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());

        private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void smartUpdate(Object handle, Map event, boolean isSnapshot) {
            UpdateCall call = new UpdateCall(handle, event, isSnapshot);

            // Add to both category-specific and chronological lists
            allUpdatesChronological.add(call);
            if (isSnapshot) {
                smartSnapshotUpdates.add(call);
            } else {
                smartRealtimeUpdates.add(call);
            }
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            smartEndOfSnapshotCalls.add(itemHandle);
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            smartClearSnapshotCalls.add(itemHandle);
        }

        @Override
        public void update(String itemName, Map event, boolean isSnapshot) {
            UpdateCall call = new UpdateCall(itemName, event, isSnapshot);

            // Add to both category-specific and chronological lists
            allUpdatesChronological.add(call);
            if (isSnapshot) {
                snapshotUpdates.add(call);
            } else {
                realtimeUpdates.add(call);
            }
        }

        @Override
        public void clearSnapshot(String itemName) {
            clearSnapshotCalls.add(itemName);
        }

        @Override
        public void endOfSnapshot(String itemName) {
            endOfSnapshotCalls.add(itemName);
        }

        @Override
        public void failure(Throwable t) {
            failures.add(t);
        }

        public List<UpdateCall> getAllUpdatesChronological() {
            return new ArrayList<>(allUpdatesChronological);
        }

        public List<UpdateCall> getSmartSnapshotUpdates() {
            return new ArrayList<>(smartSnapshotUpdates);
        }

        public List<UpdateCall> getSnapshotUpdates() {
            return new ArrayList<>(snapshotUpdates);
        }

        public List<UpdateCall> getSmartRealtimeUpdates() {
            return new ArrayList<>(smartRealtimeUpdates);
        }

        public List<UpdateCall> getRealtimeUpdates() {
            return new ArrayList<>(realtimeUpdates);
        }

        public List<Object> getSmartClearSnapshotCalls() {
            return new ArrayList<>(smartClearSnapshotCalls);
        }

        public List<String> getClearSnapshotCalls() {
            return new ArrayList<>(clearSnapshotCalls);
        }

        public List<Object> getSmartEndOfSnapshotCalls() {
            return new ArrayList<>(smartEndOfSnapshotCalls);
        }

        public List<String> getEndOfSnapshotCalls() {
            return new ArrayList<>(endOfSnapshotCalls);
        }

        public List<Throwable> getFailures() {
            return new ArrayList<>(failures);
        }

        public int getSmartRealtimeUpdateCount() {
            return smartRealtimeUpdates.size();
        }

        public int getRealtimeUpdateCount() {
            return realtimeUpdates.size();
        }

        public void reset() {
            smartSnapshotUpdates.clear();
            snapshotUpdates.clear();
            smartRealtimeUpdates.clear();
            realtimeUpdates.clear();
            allUpdatesChronological.clear();
            smartClearSnapshotCalls.clear();
            clearSnapshotCalls.clear();
            smartEndOfSnapshotCalls.clear();
            endOfSnapshotCalls.clear();
            failures.clear();
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
        public void smartUpdate(Object itemHandle, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
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
    }

    public static class RemoteTestEventListener
            implements com.lightstreamer.adapters.remote.ItemEventListener {

        private final List<UpdateCall> snapshotUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<UpdateCall> realtimeUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<UpdateCall> allUpdatesChronological =
                Collections.synchronizedList(new ArrayList<>());

        private final List<String> clearSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());

        private final List<String> endOfSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());

        private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void update(
                String itemName,
                com.lightstreamer.adapters.remote.ItemEvent itemEvent,
                boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, Map<String, ?> itemEvent, boolean isSnapshot) {
            @SuppressWarnings("unchecked")
            UpdateCall call = new UpdateCall(itemName, (Map<String, String>) itemEvent, isSnapshot);

            // Add to both category-specific and chronological lists
            allUpdatesChronological.add(call);
            if (isSnapshot) {
                snapshotUpdates.add(call);
            } else {
                realtimeUpdates.add(call);
            }
        }

        @Override
        public void update(
                String itemName,
                com.lightstreamer.adapters.remote.IndexedItemEvent itemEvent,
                boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            endOfSnapshotCalls.add(itemName);
        }

        @Override
        public void clearSnapshot(String itemName) {
            clearSnapshotCalls.add(itemName);
        }

        @Override
        public void declareFieldDiffOrder(
                String itemName,
                Map<String, com.lightstreamer.adapters.remote.DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException("Unimplemented method 'declareFieldDiffOrder'");
        }

        @Override
        public void failure(Exception exception) {
            failures.add(exception);
        }

        public List<UpdateCall> getAllUpdatesChronological() {
            return new ArrayList<>(allUpdatesChronological);
        }

        public List<UpdateCall> getSnapshotUpdates() {
            return new ArrayList<>(snapshotUpdates);
        }

        public List<UpdateCall> getRealtimeUpdates() {
            return new ArrayList<>(realtimeUpdates);
        }

        public List<String> getClearSnapshotCalls() {
            return new ArrayList<>(clearSnapshotCalls);
        }

        public List<String> getEndOfSnapshotCalls() {
            return new ArrayList<>(endOfSnapshotCalls);
        }

        public List<Throwable> getFailures() {
            return new ArrayList<>(failures);
        }

        public int getRealtimeUpdateCount() {
            return realtimeUpdates.size();
        }
    }

    public static class MockObserver implements Observer {

        @Override
        public Observer enableLatest() {
            return this;
        }

        @Override
        public Observer enableRate() {
            return this;
        }

        @Override
        public Observer enableIrate() {
            return this;
        }

        @Override
        public Observer enableAverage() {
            return this;
        }

        @Override
        public Observer enableMax() {
            return this;
        }

        @Override
        public Observer enableMin() {
            return this;
        }

        @Override
        public Observer withRangeInterval(Duration rangeInterval) {
            return this;
        }
    }

    public static class MockMonitor implements Monitor {

        @Override
        public Observer observe(Meter meter) {
            return new MockObserver();
        }

        @Override
        public void start(Duration evaluationInterval) {}

        @Override
        public void stop() {}

        @Override
        public boolean isRunning() {
            return false;
        }
    }
}
