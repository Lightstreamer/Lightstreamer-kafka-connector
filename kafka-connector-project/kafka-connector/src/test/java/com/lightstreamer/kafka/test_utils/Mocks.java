
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
import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.CanonicalItemExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.monitors.Observer;
import com.lightstreamer.kafka.common.monitors.metrics.Meter;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValueFormatter;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class Mocks {

    public static class MockConsumer
            extends org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]> {

        // Reflective handle to the inherited (private) wakeup flag. The parent MockConsumer
        // declares both poll() and wakeup() as synchronized, so a wakeup() call blocks until any
        // in-progress poll() releases the consumer monitor. A long-running scheduled poll task
        // would therefore delay the wakeup signal until after the consume loop has already
        // observed its 'closed' flag, masking the WakeupException. The real KafkaConsumer never
        // blocks wakeup() on poll(), so the override below sets the flag directly, without the
        // monitor, to faithfully reproduce that behavior.
        private static final Field WAKEUP_FIELD;

        static {
            try {
                WAKEUP_FIELD =
                        org.apache.kafka.clients.consumer.MockConsumer.class.getDeclaredField(
                                "wakeup");
                WAKEUP_FIELD.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private RuntimeException commitException;
        private KafkaException listTopicException;

        public MockConsumer(String strategyType) {
            super(strategyType);
        }

        @Override
        public void wakeup() {
            // Non-synchronized wakeup: sets the inherited flag without acquiring the consumer
            // monitor, so it cannot be blocked by an in-progress poll().
            try {
                ((AtomicBoolean) WAKEUP_FIELD.get(this)).set(true);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Unable to access MockConsumer wakeup flag", e);
            }
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
        public synchronized void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            if (commitException != null) {
                if (callback != null) {
                    callback.onComplete(offsets, commitException);
                }
                return;
            }

            super.commitAsync(offsets, callback);
        }

        @Override
        public synchronized Map<String, List<PartitionInfo>> listTopics() {
            if (listTopicException != null) {
                throw listTopicException;
            }
            return super.listTopics();
        }

        public static Function<Properties, Consumer<byte[], byte[]>> factory() {
            return factory(false);
        }

        public static Function<Properties, Consumer<byte[], byte[]>> factory(
                boolean exceptionOnConnection) {
            return prop -> consumer(exceptionOnConnection);
        }

        public static Consumer<byte[], byte[]> consumer(boolean exceptionOnConnection) {
            if (exceptionOnConnection) {
                throw new KafkaException("Simulated Exception");
            }
            return new MockConsumer(EARLIEST.toString());
        }

        public static Consumer<byte[], byte[]> consumer() {
            return consumer(false);
        }

        public static Function<Properties, Consumer<byte[], byte[]>> factory(String... topics) {
            return prop -> {
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
            records.add(ConsumedRecordInfo.from(record));
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

        public List<ConsumedRecordInfo> getConsumedRecords() {
            return records;
        }

        @Override
        public void onConsumerShutdown() {}

        @Override
        public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
            throw new UnsupportedOperationException("Unimplemented method 'offsetsSnapshot'");
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
        public void process(KafkaRecord<K, V> record, boolean isSnapshot) throws ValueException {
            if (e == null) {
                return;
            }

            if (offsetTriggeringExceptions.contains(ConsumedRecordInfo.from(record))) {
                throw e;
            }
        }

        @Override
        public void useLogger(Logger logger) {}

        public void processAsSnapshot(KafkaRecord<K, V> record, SubscribedItem subscribedItem)
                throws ValueException {
            throw new UnsupportedOperationException("Unimplemented method 'processAsSnapshot'");
        }

        @Override
        public ProcessUpdatesType processUpdatesType() {
            return this.processUpdatesType;
        }
    }

    public static class MockRecordMapper<K, V> implements RecordMapper<K, V> {

        private List<ConsumedRecordInfo> offsetTriggeringExceptions;
        private RuntimeException e;

        public MockRecordMapper(
                RuntimeException e, List<ConsumedRecordInfo> offsetTriggeringExceptions) {
            this.e = e;
            this.offsetTriggeringExceptions = offsetTriggeringExceptions;
        }

        @Override
        public Set<CanonicalItemExtractor<K, V>> getExtractorsByTopicSubscription(
                String topicName) {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'getExtractorsByTopicSubscription'");
        }

        @Override
        public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
            if (offsetTriggeringExceptions.contains(ConsumedRecordInfo.from(record))) {
                throw e;
            }
            return MappedRecord.nop();
        }

        @Override
        public boolean hasCanonicalItemExtractors() {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'hasCanonicalItemExtractors'");
        }

        @Override
        public boolean hasFieldExtractor() {
            throw new UnsupportedOperationException("Unimplemented method 'hasFieldExtractor'");
        }

        @Override
        public boolean isRegexEnabled() {
            throw new UnsupportedOperationException("Unimplemented method 'isRegexEnabled'");
        }
    }

    public static record EventCall(
            EventType type, Object handle, Map<String, String> event, boolean isSnapshot) {

        EventCall(EventType type, Object handle) {
            this(type, handle, null, false);
        }

        public static EventCall CS(Object handle) {
            return new EventCall(EventType.CS, handle);
        }

        public static EventCall EOS(Object handle) {
            return new EventCall(EventType.EOS, handle);
        }

        public enum EventType {
            UPDATE,
            EOS,
            CS
        }
    }

    /** Test double for ItemEventListener that records all method calls for verification */
    public static class MockItemEventListener implements ItemEventListener {

        private final List<EventCall> events = Collections.synchronizedList(new ArrayList<>());

        private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        private java.util.function.Consumer<String> forceSubscriptionAction = name -> {};

        public void setForceSubscriptionAction(java.util.function.Consumer<String> action) {
            this.forceSubscriptionAction = action;
        }

        @Override
        public void smartUpdate(Object handle, Map event, boolean isSnapshot) {
            events.add(new EventCall(EventCall.EventType.UPDATE, handle, event, isSnapshot));
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            EventCall call = new EventCall(EventCall.EventType.EOS, itemHandle, null, false);
            events.add(call);
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            EventCall call = new EventCall(EventCall.EventType.CS, itemHandle, null, false);
            events.add(call);
        }

        @Override
        public void unforceSubscription(String itemName) {
            // No-op for this mock, but could be extended to record forced subscriptions if needed
        }

        @Override
        public Mode forceSubscription(String itemName) {
            forceSubscriptionAction.accept(itemName);
            return Mode.MERGE;
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void failure(Throwable t) {
            failures.add(t);
        }

        public List<EventCall> getEvents() {
            return new ArrayList<>(events);
        }

        public List<EventCall> getSmartSnapshotUpdates() {
            return events.stream()
                    .filter(call -> call.type() == EventCall.EventType.UPDATE && call.isSnapshot())
                    .toList();
        }

        public List<EventCall> getSmartRealtimeUpdates() {
            return events.stream()
                    .filter(call -> call.type() == EventCall.EventType.UPDATE && !call.isSnapshot())
                    .toList();
        }

        public List<Object> getSmartClearSnapshotCalls() {
            return events.stream()
                    .filter(call -> call.type() == EventCall.EventType.CS)
                    .map(EventCall::handle)
                    .toList();
        }

        public List<Object> getSmartEndOfSnapshotCalls() {
            return events.stream()
                    .filter(call -> call.type() == EventCall.EventType.EOS)
                    .map(EventCall::handle)
                    .toList();
        }

        public List<Throwable> getFailures() {
            return new ArrayList<>(failures);
        }

        public void reset() {
            events.clear();
            forceSubscriptionAction = name -> {};
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
        public void update(String arg0, Map arg1, boolean arg2) {
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

        private final List<EventCall> snapshotUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<EventCall> realtimeUpdates =
                Collections.synchronizedList(new ArrayList<>());

        private final List<EventCall> allUpdatesChronological =
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
            EventCall call =
                    new EventCall(
                            EventCall.EventType.UPDATE,
                            itemName,
                            (Map<String, String>) itemEvent,
                            isSnapshot);

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

        public List<EventCall> getAllUpdatesChronological() {
            return new ArrayList<>(allUpdatesChronological);
        }

        public List<EventCall> getSnapshotUpdates() {
            return new ArrayList<>(snapshotUpdates);
        }

        public List<EventCall> getRealtimeUpdates() {
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

        @Override
        public Observer enableLatest(int precision, MetricValueFormatter formatter) {
            return this;
        }

        @Override
        public Observer enableRate(int precision, MetricValueFormatter formatter) {
            return this;
        }

        @Override
        public Observer enableIrate(int precision, MetricValueFormatter formatter) {
            return this;
        }

        @Override
        public Observer enableAverage(int precision, MetricValueFormatter formatter) {
            return this;
        }

        @Override
        public Observer enableMax(int precision, MetricValueFormatter formatter) {
            return this;
        }

        @Override
        public Observer enableMin(int precision, MetricValueFormatter formatter) {
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
