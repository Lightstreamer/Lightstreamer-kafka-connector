
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.snapshot;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.namedFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.UpdateCall;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TopicSnapshotConsumerTest {

    private static final String TOPIC = "stocks";
    private static final org.slf4j.Logger logger =
            LoggerFactory.getLogger(TopicSnapshotConsumerTest.class);

    private static final Serializer<String> KEY_SER = Serdes.String().serializer();
    private static final Serializer<String> VAL_SER = Serdes.String().serializer();

    private MockItemEventListener mockListener;
    private EventListener eventListener;
    private DeserializerPair<String, String> deserializerPair;
    private RecordMapper<String, String> recordMapper;

    @BeforeEach
    void setUp() {
        mockListener = new MockItemEventListener();
        eventListener = EventListener.smartEventListener(mockListener);
        deserializerPair =
                new DeserializerPair<>(
                        Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    // --- Helpers ---

    private RecordMapper<String, String> createMapper(String templateExpr)
            throws ExtractionException {
        ItemTemplateConfigs templateConfigs = ItemTemplateConfigs.from(Map.of("t", templateExpr));
        TopicMappingConfig topicMapping =
                TopicMappingConfig.fromDelimitedMappings(TOPIC, "item-template.t");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(topicMapping));
        ItemTemplates<String, String> templates =
                Items.templatesFrom(topicsConfig, OthersSelectorSuppliers.String());
        return RecordMapper.from(
                templates,
                namedFieldsExtractor(
                        OthersSelectorSuppliers.String(),
                        Map.of("value", Wrapped("#{VALUE}")),
                        false,
                        false));
    }

    private Supplier<Consumer<byte[], byte[]>> mockConsumerSupplier(
            int partitions, List<ConsumerRecord<byte[], byte[]>> records) {
        return () -> {
            // Subclass that adds records after assign() is called,
            // because MockConsumer only returns records for assigned partitions.
            MockConsumer<byte[], byte[]> mock =
                    new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
                        @Override
                        public synchronized void assign(
                                java.util.Collection<TopicPartition> partitions) {
                            super.assign(partitions);
                            for (ConsumerRecord<byte[], byte[]> r : records) {
                                addRecord(r);
                            }
                        }
                    };

            // Set up partition info
            List<PartitionInfo> partInfos = new java.util.ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                partInfos.add(new PartitionInfo(TOPIC, p, null, null, null));
            }
            mock.updatePartitions(TOPIC, partInfos);

            // Compute end offsets per partition
            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            for (int p = 0; p < partitions; p++) {
                endOffsets.put(new TopicPartition(TOPIC, p), 0L);
            }
            for (ConsumerRecord<byte[], byte[]> r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                endOffsets.merge(tp, r.offset() + 1, Long::max);
            }
            mock.updateEndOffsets(endOffsets);

            // Set beginning offsets
            Map<TopicPartition, Long> beginOffsets = new HashMap<>();
            for (int p = 0; p < partitions; p++) {
                beginOffsets.put(new TopicPartition(TOPIC, p), 0L);
            }
            mock.updateBeginningOffsets(beginOffsets);

            return mock;
        };
    }

    private ConsumerRecord<byte[], byte[]> record(
            String key, String value, int partition, long offset) {
        return new ConsumerRecord<>(
                TOPIC,
                partition,
                offset,
                KEY_SER.serialize(TOPIC, key),
                VAL_SER.serialize(TOPIC, value));
    }

    /**
     * Creates a listener that directly finalizes items and optionally tracks the completed topic.
     */
    private TopicSnapshotConsumer.SnapshotListener snapshotListener(
            java.util.function.Consumer<String> onCompleted) {
        return new TopicSnapshotConsumer.SnapshotListener() {
            @Override
            public void onSnapshotConsumerCompleted(String topic) {
                onCompleted.accept(topic);
            }

            @Override
            public void onItemComplete(SubscribedItem item) {
                item.endOfSnapshot(eventListener);
                item.enableRealtimeEvents(eventListener);
            }
        };
    }

    /** Creates a listener that directly finalizes items without tracking completion. */
    private TopicSnapshotConsumer.SnapshotListener snapshotListener() {
        return snapshotListener(t -> {});
    }

    private void runAndWait(TopicSnapshotConsumer<String, String> consumer) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                consumer.run();
                            } finally {
                                latch.countDown();
                            }
                        });
        thread.setDaemon(true);
        thread.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    // --- Tests ---

    @Test
    public void shouldDeliverSnapshotToSingleItem() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        List<ConsumerRecord<byte[], byte[]>> records =
                List.of(record("AAPL", "150.0", 0, 0), record("GOOG", "2800.0", 0, 1));

        AtomicReference<String> completedTopic = new AtomicReference<>();
        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, records),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener(completedTopic::set));

        SubscribedItem item = Items.subscribedFrom("stock-[symbol=AAPL]", "handle1");
        consumer.enqueue(item);

        runAndWait(consumer);

        // Should receive exactly one snapshot event for AAPL
        List<UpdateCall> snapshots = mockListener.getSmartSnapshotUpdates();
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots.get(0).handle()).isEqualTo("handle1");
        assertThat(snapshots.get(0).event()).containsEntry("value", "150.0");

        // End of snapshot should be signalled
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("handle1");

        // Completion callback should fire
        assertThat(completedTopic.get()).isEqualTo(TOPIC);
    }

    @Test
    public void shouldDeliverSnapshotToMultipleItems() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        List<ConsumerRecord<byte[], byte[]>> records =
                List.of(
                        record("AAPL", "150.0", 0, 0),
                        record("GOOG", "2800.0", 0, 1),
                        record("MSFT", "300.0", 0, 2));

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, records),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        SubscribedItem itemAapl = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        SubscribedItem itemGoog = Items.subscribedFrom("stock-[symbol=GOOG]", "h2");
        consumer.enqueue(itemAapl);
        consumer.enqueue(itemGoog);

        runAndWait(consumer);

        // Both items should get their matching snapshot event
        List<UpdateCall> snapshots = mockListener.getSmartSnapshotUpdates();
        assertThat(snapshots).hasSize(2);

        Map<Object, String> handleToValue = new HashMap<>();
        for (UpdateCall call : snapshots) {
            handleToValue.put(call.handle(), call.event().get("value"));
        }
        assertThat(handleToValue).containsEntry("h1", "150.0");
        assertThat(handleToValue).containsEntry("h2", "2800.0");

        // End of snapshot for both items
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1", "h2");
    }

    @Test
    public void shouldHandleNoMatchingRecords() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        List<ConsumerRecord<byte[], byte[]>> records = List.of(record("GOOG", "2800.0", 0, 0));

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, records),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        // Subscribe to AAPL but only GOOG is in the topic
        SubscribedItem item = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        consumer.enqueue(item);

        runAndWait(consumer);

        // No snapshot events delivered, but end of snapshot still signalled
        assertThat(mockListener.getSmartSnapshotUpdates()).isEmpty();
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1");
    }

    @Test
    public void shouldHandleEmptyTopic() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, List.of()),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        SubscribedItem item = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        consumer.enqueue(item);

        runAndWait(consumer);

        // Empty topic: no snapshot events, end of snapshot still fires
        assertThat(mockListener.getSmartSnapshotUpdates()).isEmpty();
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1");
    }

    @Test
    public void shouldHandleNoPartitions() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        // Supplier returns a consumer with no partitions for the topic
        Supplier<Consumer<byte[], byte[]>> emptySupplier =
                () -> {
                    MockConsumer<byte[], byte[]> mock =
                            new MockConsumer<>(OffsetResetStrategy.EARLIEST);
                    // No partitions registered
                    return mock;
                };

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        emptySupplier,
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        SubscribedItem item = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        consumer.enqueue(item);

        runAndWait(consumer);

        // No partitions: item finalized with empty snapshot
        assertThat(mockListener.getSmartSnapshotUpdates()).isEmpty();
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1");
    }

    @Test
    public void shouldExitWhenNoPendingItems() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        AtomicReference<String> completedTopic = new AtomicReference<>();
        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, List.of()),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener(completedTopic::set));

        // Don't enqueue anything — consumer should exit immediately
        runAndWait(consumer);

        assertThat(mockListener.getSmartSnapshotUpdates()).isEmpty();
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).isEmpty();
        assertThat(completedTopic.get()).isEqualTo(TOPIC);
    }

    @Test
    public void shouldDeliverSnapshotAcrossMultiplePartitions() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        List<ConsumerRecord<byte[], byte[]>> records =
                List.of(record("AAPL", "150.0", 0, 0), record("GOOG", "2800.0", 1, 0));

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(2, records),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        SubscribedItem itemAapl = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        SubscribedItem itemGoog = Items.subscribedFrom("stock-[symbol=GOOG]", "h2");
        consumer.enqueue(itemAapl);
        consumer.enqueue(itemGoog);

        runAndWait(consumer);

        List<UpdateCall> snapshots = mockListener.getSmartSnapshotUpdates();
        assertThat(snapshots).hasSize(2);

        Map<Object, String> handleToValue = new HashMap<>();
        for (UpdateCall call : snapshots) {
            handleToValue.put(call.handle(), call.event().get("value"));
        }
        assertThat(handleToValue).containsEntry("h1", "150.0");
        assertThat(handleToValue).containsEntry("h2", "2800.0");
    }

    @Test
    public void shouldInvokeShutdownGracefully() throws Exception {
        recordMapper = createMapper("stock-#{symbol=KEY}");

        // Create a consumer that will block on poll (no records, large end offset)
        Supplier<Consumer<byte[], byte[]>> blockingSupplier =
                () -> {
                    MockConsumer<byte[], byte[]> mock =
                            new MockConsumer<>(OffsetResetStrategy.EARLIEST);
                    mock.updatePartitions(
                            TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
                    Map<TopicPartition, Long> endOffsets =
                            Map.of(new TopicPartition(TOPIC, 0), 1000L);
                    mock.updateEndOffsets(endOffsets);
                    mock.updateBeginningOffsets(Map.of(new TopicPartition(TOPIC, 0), 0L));
                    // No records added — poll will return empty, loop keeps going
                    return mock;
                };

        AtomicReference<String> completedTopic = new AtomicReference<>();
        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        blockingSupplier,
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener(completedTopic::set));

        SubscribedItem item = Items.subscribedFrom("stock-[symbol=AAPL]", "h1");
        consumer.enqueue(item);

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                consumer.run();
                            } finally {
                                latch.countDown();
                            }
                        });
        thread.setDaemon(true);
        thread.start();

        // Give the consumer time to enter its poll loop
        Thread.sleep(200);

        // Trigger shutdown
        consumer.shutdown();

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(completedTopic.get()).isEqualTo(TOPIC);

        // The item should still be finalized (drainAndFinalizeAll)
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1");
    }

    @Test
    public void shouldDeliverSnapshotForSimpleItem() throws Exception {
        // Simple item (no parameters) — all records match
        ItemTemplateConfigs templateConfigs = ItemTemplateConfigs.empty();
        TopicMappingConfig topicMapping =
                TopicMappingConfig.fromDelimitedMappings(TOPIC, "all-stocks");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(topicMapping));
        ItemTemplates<String, String> templates =
                Items.templatesFrom(topicsConfig, OthersSelectorSuppliers.String());
        recordMapper =
                RecordMapper.from(
                        templates,
                        namedFieldsExtractor(
                                OthersSelectorSuppliers.String(),
                                Map.of("value", Wrapped("#{VALUE}")),
                                false,
                                false));

        List<ConsumerRecord<byte[], byte[]>> records =
                List.of(record("AAPL", "150.0", 0, 0), record("GOOG", "2800.0", 0, 1));

        TopicSnapshotConsumer<String, String> consumer =
                new TopicSnapshotConsumer<>(
                        TOPIC,
                        logger,
                        recordMapper,
                        eventListener,
                        mockConsumerSupplier(1, records),
                        deserializerPair,
                        CommandModeStrategy.NONE,
                        Duration.ZERO,
                        snapshotListener());

        SubscribedItem item = Items.subscribedFrom("all-stocks", "h1");
        consumer.enqueue(item);

        runAndWait(consumer);

        // Simple items receive ALL records
        List<UpdateCall> snapshots = mockListener.getSmartSnapshotUpdates();
        assertThat(snapshots).hasSize(2);
        assertThat(mockListener.getSmartEndOfSnapshotCalls()).containsExactly("h1");
    }
}
