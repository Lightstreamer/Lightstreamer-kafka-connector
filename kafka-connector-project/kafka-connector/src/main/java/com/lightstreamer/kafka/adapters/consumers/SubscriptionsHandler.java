
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

package com.lightstreamer.kafka.adapters.consumers;

import static org.apache.kafka.common.serialization.Serdes.ByteArray;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.snapshot.TopicSnapshotConsumer;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public interface SubscriptionsHandler<K, V> {

    void subscribe(String item, Object itemHandle) throws SubscriptionException;

    Optional<SubscribedItem> unsubscribe(String topic) throws SubscriptionException;

    boolean isConsuming();

    void setListener(ItemEventListener listener);

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class Builder<K, V> {

        private static final Deserializer<byte[]> BYTE_ARRAY_DESERIALIZER =
                ByteArray().deserializer();

        private Config<K, V> consumerConfig;
        private MetadataListener metadataListener;
        private Supplier<Consumer<byte[], byte[]>> consumerSupplier;

        private Builder() {}

        public Builder<K, V> withConsumerConfig(Config<K, V> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }

        public Builder<K, V> withMetadataListener(MetadataListener metadataListener) {
            this.metadataListener = metadataListener;
            return this;
        }

        public Builder<K, V> withConsumerSupplier(
                Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
            this.consumerSupplier = consumerSupplier;
            return this;
        }

        public SubscriptionsHandler<K, V> build() {
            this.consumerSupplier =
                    Objects.requireNonNullElse(
                            this.consumerSupplier, defaultConsumerSupplier(consumerConfig));
            return new DefaultSubscriptionsHandler<>(this);
        }

        private static <K, V> Supplier<Consumer<byte[], byte[]>> defaultConsumerSupplier(
                Config<K, V> config) {
            return () ->
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            BYTE_ARRAY_DESERIALIZER,
                            BYTE_ARRAY_DESERIALIZER);
        }
    }

    static class DefaultSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        private final Config<K, V> config;
        private final MetadataListener metadataListener;
        private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;

        private final Logger logger;
        private final RecordMapper<K, V> recordMapper;
        private final KafkaRecord.DeserializerPair<K, V> deserializerPair;
        private final ExecutorService pool;

        private final SubscribedItems subscribedItems;
        private final ReentrantLock consumerLock = new ReentrantLock();

        /** Active snapshot consumers keyed by topic. */
        private final Map<String, TopicSnapshotConsumer<K, V>> snapshotConsumers =
                new ConcurrentHashMap<>();

        /**
         * Tracks how many topic consumers still need to complete for each item. When the count
         * reaches zero, the item is finalized (end of snapshot + enable real-time).
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

        private KafkaConsumerWrapper<K, V> consumer; // guarded by consumerLock
        private FutureStatus futureStatus; // guarded by consumerLock
        private int itemsCount; // guarded by consumerLock

        private EventListener eventListener;

        // Only for testing purposes: hook invoked before acquiring lock in
        // decrementAndMaybeStopConsuming()
        Runnable stopConsumingHook = () -> {};

        DefaultSubscriptionsHandler(Builder<K, V> builder) {
            this.config = builder.consumerConfig;
            this.metadataListener = builder.metadataListener;
            this.consumerSupplier = builder.consumerSupplier;
            this.logger = LogFactory.getLogger(config.connectionName());
            this.recordMapper =
                    RecordMapper.from(config.itemTemplates(), config.fieldsExtractor());
            this.deserializerPair =
                    new KafkaRecord.DeserializerPair<>(
                            config.suppliers().keySelectorSupplier().deserializer(),
                            config.suppliers().valueSelectorSupplier().deserializer());
            this.pool =
                    Executors.newSingleThreadExecutor(r -> new Thread(r, "SubscriptionHandler"));
            this.subscribedItems = SubscribedItems.create();
        }

        @Override
        public void setListener(ItemEventListener listener) {
            if (listener == null) {
                throw new IllegalArgumentException("ItemEventListener cannot be null");
            }
            this.eventListener = EventListener.smartEventListener(listener);
        }

        @Override
        public void subscribe(String item, Object itemHandle) throws SubscriptionException {
            try {
                SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
                if (!config.itemTemplates().matches(newItem)) {
                    logger.atWarn()
                            .log("Item [{}] does not match any defined item templates", item);
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }

                logger.atInfo().log("Subscribed to item [{}]", item);

                subscribedItems.addItem(newItem);

                // TODO: temporarily always enabled; gate on a snapshot.enable configuration
                // property. When disabled, call item.enableRealtimeEvents(eventListener) instead.
                incrementAndMaybeStartConsuming();
                enqueueForSnapshot(newItem);
            } catch (ExpressionException e) {
                logger.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        @Override
        public Optional<SubscribedItem> unsubscribe(String item) {
            Optional<SubscribedItem> removedItem = subscribedItems.removeItem(item);
            if (removedItem.isPresent()) {
                decrementAndMaybeStopConsuming();
            }

            return removedItem;
        }

        private void incrementAndMaybeStartConsuming() {
            logger.atTrace().log("Acquiring consumer lock to start consuming events...");
            consumerLock.lock();
            logger.atTrace().log("Consumer lock acquired");
            try {
                itemsCount++;
                if (itemsCount == 1) {
                    logger.atInfo().log("Consumer not yet initialized, creating a new one...");
                    consumer = newConsumer(); // May throw KafkaException
                    logger.atInfo().log("New consumer connecting and subscribing...");
                    futureStatus = consumer.startLoop(pool);
                } else {
                    logger.atDebug().log("Consumer is already consuming events, nothing to do");
                }
            } catch (KafkaException ke) {
                logger.atError().setCause(ke).log("Unable to connect to Kafka");
                metadataListener.forceUnsubscriptionAll();
            } finally {
                logger.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                logger.atTrace().log("Consumer lock released");
            }
        }

        private void decrementAndMaybeStopConsuming() {
            stopConsumingHook.run();
            logger.atTrace().log("Acquiring consumer lock to stop consuming...");
            consumerLock.lock();
            logger.atTrace().log("Consumer lock acquired to stop consuming");
            try {
                itemsCount--;
                if (itemsCount == 0) {
                    shutdownSnapshotConsumers();
                    if (consumer != null) {
                        logger.atInfo().log("Stopping consumer...");
                        consumer.shutdown();
                        consumer = null;
                        futureStatus = null;
                        logger.atInfo().log("Consumer stopped");
                    } else {
                        logger.atDebug().log("Consumer was not initialized, nothing to do");
                    }
                } else {
                    logger.atDebug().log("Consumer still has active subscriptions, nothing to do");
                }
            } finally {
                logger.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                logger.atTrace().log("Consumer lock released");
            }
        }

        @Override
        public boolean isConsuming() {
            consumerLock.lock();
            try {
                return consumer != null && !futureStatus.isStateAvailable();
            } finally {
                consumerLock.unlock();
            }
        }

        private KafkaConsumerWrapper<K, V> newConsumer() throws KafkaException {
            if (eventListener == null) {
                throw new RuntimeException(
                        "EventListener must be set before starting the consumer");
            }
            return new KafkaConsumerWrapper<>(
                    config,
                    metadataListener,
                    eventListener,
                    subscribedItems,
                    recordMapper,
                    consumerSupplier);
        }

        /**
         * Enqueues an item for snapshot delivery. For each configured topic, gets or creates a
         * {@link TopicSnapshotConsumer} and enqueues the item. The consumer is launched on first
         * creation and will self-remove via the completion callback when done.
         *
         * <p>Falls back to immediate real-time mode when regex topic subscriptions are active,
         * since snapshot requires literal topic names to resolve partitions.
         */
        private void enqueueForSnapshot(SubscribedItem item) {
            // TODO: validate at config time when snapshot.enable is gated by configuration
            if (config.itemTemplates().isRegexEnabled()) {
                logger.atWarn()
                        .log(
                                "Snapshot not supported with regex topic subscriptions, "
                                        + "falling back to real-time for item [{}]",
                                item.name());
                item.enableRealtimeEvents(eventListener);
                return;
            }

            Set<String> topics = config.itemTemplates().topicsFor(item);
            if (topics.isEmpty()) {
                item.enableRealtimeEvents(eventListener);
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
                                    return createSnapshotConsumer(t);
                                });
                snapshotConsumer.enqueue(item);
                if (created[0]) {
                    snapshotPool.submit(snapshotConsumer);
                }
            }
        }

        private TopicSnapshotConsumer<K, V> createSnapshotConsumer(String topic) {
            logger.atInfo().log("Creating snapshot consumer for topic [{}]", topic);
            return new TopicSnapshotConsumer<>(
                    topic,
                    logger,
                    recordMapper,
                    eventListener,
                    consumerSupplier,
                    deserializerPair,
                    config.commandModeStrategy(),
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
                            onItemSnapshotComplete(item);
                        }
                    });
        }

        /**
         * Called by each {@link TopicSnapshotConsumer} when it has finished processing an item.
         * Decrements the per-item topic counter; when the last topic completes, the item is
         * finalized with end of snapshot and real-time events are enabled.
         */
        private void onItemSnapshotComplete(SubscribedItem item) {
            AtomicInteger remaining = pendingSnapshotTopics.get(item);
            if (remaining == null || remaining.decrementAndGet() > 0) {
                return;
            }
            pendingSnapshotTopics.remove(item);
            try {
                item.endOfSnapshot(eventListener);
                item.enableRealtimeEvents(eventListener);
                logger.atDebug().log("Snapshot finalized for item [{}]", item.name());
            } catch (Exception e) {
                logger.atError()
                        .setCause(e)
                        .log("Error finalizing snapshot for item [{}]", item.name());
                try {
                    item.enableRealtimeEvents(eventListener);
                } catch (Exception inner) {
                    logger.atError()
                            .setCause(inner)
                            .log("Failed to enable real-time events for item [{}]", item.name());
                }
            }
        }

        private void shutdownSnapshotConsumers() {
            snapshotConsumers.values().forEach(TopicSnapshotConsumer::shutdown);
            snapshotConsumers.clear();
        }

        // Only for testing purposes
        SubscribedItems getSubscribedItems() {
            return subscribedItems;
        }

        // Only for testing purposes
        FutureStatus getFutureStatus() {
            return futureStatus;
        }

        // Only for testing purposes
        int getItemsCounter() {
            consumerLock.lock();
            try {
                return itemsCount;
            } finally {
                consumerLock.unlock();
            }
        }
    }
}
