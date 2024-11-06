
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

package com.lightstreamer.kafka.adapters.consumers;

import com.lightstreamer.interfaces.data.DiffAlgorithm;
import com.lightstreamer.interfaces.data.IndexedItemEvent;
import com.lightstreamer.interfaces.data.ItemEvent;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService.OffsetStoreSupplier;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

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
import java.util.function.Consumer;

public class Fakes {

    public static class FakeKafkaConsumer<K, V> implements ConsumerWrapper<K, V> {

        private volatile boolean ran = false;
        private volatile boolean closed = false;

        FakeKafkaConsumer() {}

        @Override
        public void run() {
            ran = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean hasRan() {
            return ran;
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public void consumeRecords(ConsumerRecords<K, V> records) {
            throw new UnsupportedOperationException("Unimplemented method 'consumeRecords'");
        }
    }

    public static class FakeMetadataListener implements MetadataListener {

        private volatile boolean forcedUnsubscription = false;

        @Override
        public void forceUnsubscription(String item) {}

        @Override
        public void forceUnsubscriptionAll() {
            forcedUnsubscription = true;
        }

        boolean forcedUnsubscription() {
            return forcedUnsubscription;
        }
    }

    public static class FakeOffsetService implements OffsetService {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

        @Override
        public void commitSync() {}

        @Override
        public void commitAsync() {}

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {}

        @Override
        public void onAsyncFailure(ValueException ve) {}

        @Override
        public ValueException getFirstFailure() {
            return null;
        }

        @Override
        public void initStore(boolean flag, OffsetStoreSupplier storeSupplier) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }

        @Override
        public boolean isNotAlreadyConsumed(ConsumerRecord<?, ?> record) {
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
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }
    }

    public static class FakeOffsetStore implements OffsetStore {

        private final List<ConsumerRecord<?, ?>> records = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> topicMap;

        public FakeOffsetStore(Map<TopicPartition, OffsetAndMetadata> topicMap) {
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

    public static class FakeRecordProcessor<K, V> implements RecordProcessor<K, V> {

        @Override
        public void process(ConsumerRecord<K, V> record) throws ValueException {}

        @Override
        public void useLogger(Logger logger) {}
    }

    public static class FakteItemEventListener implements ItemEventListener {

        private static Consumer<Map<String, String>> NOPConsumer = m -> {};

        private Consumer<Map<String, String>> consumer;

        public FakteItemEventListener(Consumer<Map<String, String>> consumer) {
            this.consumer = consumer;
        }

        public FakteItemEventListener() {
            this(NOPConsumer);
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
            consumer.accept(event);
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
            throw new UnsupportedOperationException("Unimplemented method 'smartEndOfSnapshot'");
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            throw new UnsupportedOperationException("Unimplemented method 'smartClearSnapshot'");
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
