
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

import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;

import com.lightstreamer.interfaces.data.DiffAlgorithm;
import com.lightstreamer.interfaces.data.IndexedItemEvent;
import com.lightstreamer.interfaces.data.ItemEvent;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper.AdminInterface;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;

public class Mocks {

    public static class MockTriggerConfig implements ConsumerTriggerConfig<String, String> {

        private final TopicConfigurations topicsConfig;
        private final Properties consumerProperties;
        private final boolean enforceCommandMode;

        public MockTriggerConfig(TopicConfigurations topicsConfig) {
            this(topicsConfig, new Properties(), false);
        }

        public MockTriggerConfig(
                TopicConfigurations topicsConfig,
                Properties properties,
                boolean enforceCommandMode) {
            this.topicsConfig = topicsConfig;
            this.consumerProperties = properties;
            this.enforceCommandMode = enforceCommandMode;
        }

        @Override
        public Properties consumerProperties() {
            return this.consumerProperties;
        }

        @Override
        public DataExtractor<String, String> fieldsExtractor() {
            try {
                return FieldConfigs.from(Map.of("field", "#{VALUE}"))
                        .extractor(String(), false, false);
            } catch (ExtractionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ItemTemplates<String, String> itemTemplates() {
            try {
                return Items.templatesFrom(topicsConfig, String());
            } catch (ExtractionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String connectionName() {
            return "TestConnection";
        }

        @Override
        public KeyValueDeserializers<String, String> deserializers() {
            return null;
        }

        @Override
        public Concurrency concurrency() {
            return new Concurrency() {

                @Override
                public RecordConsumeWithOrderStrategy orderStrategy() {
                    return RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION;
                }

                @Override
                public int threads() {
                    return 2;
                }
            };
        }

        @Override
        public RecordErrorHandlingStrategy errorHandlingStrategy() {
            return RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
        }

        @Override
        public boolean isCommandEnforceEnabled() {
            return enforceCommandMode;
        }
    }

    public static class MockConsumerWrapper<K, V> implements ConsumerWrapper<K, V> {

        private volatile boolean ran = false;
        private volatile boolean closed = false;

        public MockConsumerWrapper() {}

        @Override
        public void run() {
            ran = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean hasRan() {
            return ran;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void consumeRecords(ConsumerRecords<K, V> records) {
            throw new UnsupportedOperationException("Unimplemented method 'consumeRecords'");
        }
    }

    public static class MockAdminInterface implements AdminInterface {

        private final Set<String> topics;
        private final boolean throwException;

        public MockAdminInterface(Set<String> topics, boolean throwException) {
            this.topics = topics;
            this.throwException = throwException;
        }

        public MockAdminInterface(Set<String> topics) {
            this(topics, false);
        }

        @Override
        public void close() throws Exception {}

        @Override
        public Set<String> listTopics(ListTopicsOptions options) throws Exception {
            if (throwException) {
                throw new RuntimeException("Fake Exception");
            }
            return topics;
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

        private List<ConsumedRecordInfo> records = Collections.synchronizedList(new ArrayList<>());
        private volatile Throwable firstFailure;

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
    }

    public static class MockOffsetStore implements OffsetStore {

        private final List<ConsumerRecord<?, ?>> records = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> topicMap;

        public MockOffsetStore(Map<TopicPartition, OffsetAndMetadata> topicMap) {
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

        public MockRecordProcessor(
                RuntimeException e, List<ConsumedRecordInfo> offsetTriggeringExceptions) {
            this.e = e;
            this.offsetTriggeringExceptions = offsetTriggeringExceptions;
        }

        public MockRecordProcessor() {
            this(null, Collections.emptyList());
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
    }

    public static class MockItemEventListener implements ItemEventListener {

        private static BiConsumer<Map<String, String>, Boolean> NOPConsumer = (m, s) -> {};

        private BiConsumer<Map<String, String>, Boolean> consumer;

        boolean smartClearSnapshotCalled = false;
        boolean smartEndOfSnapshotCalled = false;

        public MockItemEventListener(BiConsumer<Map<String, String>, Boolean> consumer) {
            this.consumer = consumer;
        }

        public MockItemEventListener() {
            this(NOPConsumer);
        }

        public boolean smartClearSnapshotCalled() {
            return smartClearSnapshotCalled;
        }

        public boolean smartEndOfSnapshotCalled() {
            return smartEndOfSnapshotCalled;
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
        public void smartUpdate(Object itemHandle, Map event, boolean isSnapshot) {
            consumer.accept(event, isSnapshot);
        }

        @Override
        public void smartUpdate(Object itemHandle, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            this.smartEndOfSnapshotCalled = true;
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
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
