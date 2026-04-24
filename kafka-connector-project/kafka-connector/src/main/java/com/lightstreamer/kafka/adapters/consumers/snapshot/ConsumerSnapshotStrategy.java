
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

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A {@link SnapshotDeliveryStrategy} that uses ephemeral per-topic {@link TopicSnapshotConsumer}
 * instances to deliver snapshot data. Each topic consumer reads the full compacted state from the
 * beginning, routes matching records to subscribed items, and completes after all pending items
 * have been served.
 *
 * <p>Items arriving while a topic consumer is running are queued and served in subsequent passes.
 * When all topic consumers for an item have completed, the item is finalized with {@link
 * SubscribedItem#endOfSnapshot(EventListener) endOfSnapshot} and {@link
 * SubscribedItem#enableRealtimeEvents(EventListener) enableRealtimeEvents}.
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 */
public class ConsumerSnapshotStrategy<K, V> implements SnapshotDeliveryStrategy<K, V> {

    private final Logger logger;
    private final ItemTemplates<K, V> itemTemplates;
    private final RecordMapper<K, V> recordMapper;
    private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;
    private final KafkaRecord.DeserializerPair<K, V> deserializerPair;
    private final CommandModeStrategy commandModeStrategy;

    /** Active snapshot consumers keyed by topic. */
    private final Map<String, TopicSnapshotConsumer<K, V>> snapshotConsumers =
            new ConcurrentHashMap<>();

    /**
     * Tracks how many topic consumers still need to complete for each item. When the count reaches
     * zero, the item is finalized (end of snapshot + enable real-time).
     */
    private final Map<SubscribedItem, AtomicInteger> pendingSnapshotTopics =
            new ConcurrentHashMap<>();

    /** Thread pool for running {@link TopicSnapshotConsumer} instances. Uses daemon threads. */
    private final ExecutorService snapshotPool =
            Executors.newCachedThreadPool(
                    r -> {
                        Thread t = new Thread(r, "SnapshotConsumer");
                        t.setDaemon(true);
                        return t;
                    });

    /**
     * Constructs a new {@code ConsumerSnapshotStrategy}.
     *
     * @param connectionSpec the snapshot connection specification providing templates and
     *     deserializers
     * @param recordMapper the mapper for converting Kafka records to item events
     * @param consumerSupplier factory for creating Kafka consumers for snapshot reads
     */
    public ConsumerSnapshotStrategy(
            SnapshotConnectionSpec<K, V> connectionSpec,
            RecordMapper<K, V> recordMapper,
            Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
        this.logger = LogFactory.getLogger(connectionSpec.connectionName());
        this.itemTemplates = connectionSpec.itemTemplates();
        this.recordMapper = recordMapper;
        this.consumerSupplier = consumerSupplier;
        this.deserializerPair = connectionSpec.deserializerPair();
        this.commandModeStrategy = connectionSpec.commandModeStrategy();
    }

    @Override
    public void deliverSnapshot(SubscribedItem item, EventListener listener) {
        // TODO: validate at config time when snapshot.enable is gated by configuration
        if (itemTemplates.isRegexEnabled()) {
            logger.atWarn()
                    .log(
                            "Snapshot not supported with regex topic subscriptions, "
                                    + "falling back to real-time for item [{}]",
                            item.name());
            item.enableRealtimeEvents(listener);
            return;
        }

        Set<String> topics = itemTemplates.topicsFor(item);
        if (topics.isEmpty()) {
            item.enableRealtimeEvents(listener);
            return;
        }

        // Register the number of topics this item must wait for before finalization
        pendingSnapshotTopics.put(item, new AtomicInteger(topics.size()));

        for (String topic : topics) {
            boolean[] created = {false};
            TopicSnapshotConsumer<K, V> snapshotConsumer =
                    snapshotConsumers.computeIfAbsent(
                            topic,
                            t -> {
                                created[0] = true;
                                return createSnapshotConsumer(t, listener);
                            });
            snapshotConsumer.enqueue(item);
            if (created[0]) {
                snapshotPool.submit(snapshotConsumer);
            }
        }
    }

    private TopicSnapshotConsumer<K, V> createSnapshotConsumer(
            String topic, EventListener listener) {
        logger.atInfo().log("Creating snapshot consumer for topic [{}]", topic);
        return new TopicSnapshotConsumer<>(
                topic,
                logger,
                recordMapper,
                listener,
                consumerSupplier,
                deserializerPair,
                commandModeStrategy,
                Duration.ZERO,
                new TopicSnapshotConsumer.SnapshotListener() {
                    @Override
                    public void onSnapshotConsumerCompleted(String completedTopic) {
                        logger.atInfo().log(
                                "Snapshot consumer for topic [{}] completed", completedTopic);
                        snapshotConsumers.remove(completedTopic);
                    }

                    @Override
                    public void onItemComplete(SubscribedItem item) {
                        onItemSnapshotComplete(item, listener);
                    }
                });
    }

    /**
     * Called by each {@link TopicSnapshotConsumer} when it has finished processing an item.
     * Decrements the per-item topic counter; when the last topic completes, the item is finalized
     * with end of snapshot and real-time events are enabled.
     */
    private void onItemSnapshotComplete(SubscribedItem item, EventListener listener) {
        AtomicInteger remaining = pendingSnapshotTopics.get(item);
        if (remaining == null || remaining.decrementAndGet() > 0) {
            return;
        }
        pendingSnapshotTopics.remove(item);
        try {
            item.endOfSnapshot(listener);
            item.enableRealtimeEvents(listener);
            logger.atDebug().log("Snapshot finalized for item [{}]", item.name());
        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Error finalizing snapshot for item [{}]", item.name());
            try {
                item.enableRealtimeEvents(listener);
            } catch (Exception inner) {
                logger.atError()
                        .setCause(inner)
                        .log("Failed to enable real-time events for item [{}]", item.name());
            }
        }
    }

    @Override
    public void onAllItemsUnsubscribed() {
        stopSnapshotConsumers();
    }

    @Override
    public void shutdown() {
        stopSnapshotConsumers();
        snapshotPool.shutdown();
    }

    private void stopSnapshotConsumers() {
        snapshotConsumers.values().forEach(TopicSnapshotConsumer::shutdown);
        snapshotConsumers.clear();
    }
}
