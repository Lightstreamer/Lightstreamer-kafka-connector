
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.snapshot.CacheSnapshotStrategy;
import com.lightstreamer.kafka.adapters.consumers.snapshot.ConsumerSnapshotStrategy;
import com.lightstreamer.kafka.adapters.consumers.snapshot.NoSnapshotStrategy;
import com.lightstreamer.kafka.adapters.consumers.snapshot.SnapshotConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.snapshot.SnapshotDeliveryStrategy;
import com.lightstreamer.kafka.adapters.consumers.snapshot.SnapshotDeliveryStrategy.SnapshotMode;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

        private ConnectionSpec<K, V> connectionSpec;
        private MetadataListener metadataListener;
        private Supplier<Consumer<byte[], byte[]>> consumerSupplier;
        private Supplier<Consumer<byte[], byte[]>> snapshotConsumerSupplier;
        private SnapshotMode snapshotMode = SnapshotMode.DISABLED;

        // Package-private: allows tests to inject a custom strategy directly
        SnapshotDeliveryStrategy<K, V> snapshotStrategy;

        private Builder() {}

        public Builder<K, V> withConnectionSpec(ConnectionSpec<K, V> connectionSpec) {
            this.connectionSpec = connectionSpec;
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

        public Builder<K, V> withSnapshotConsumerSupplier(
                Supplier<Consumer<byte[], byte[]>> snapshotConsumerSupplier) {
            this.snapshotConsumerSupplier = snapshotConsumerSupplier;
            return this;
        }

        public Builder<K, V> withSnapshotMode(SnapshotMode snapshotMode) {
            this.snapshotMode = snapshotMode;
            return this;
        }

        public SubscriptionsHandler<K, V> build() {
            if (connectionSpec == null) throw new IllegalStateException("ConnectionSpec not set");
            if (metadataListener == null)
                throw new IllegalStateException("MetadataListener not set");
            if (consumerSupplier == null)
                throw new IllegalStateException("ConsumerSupplier not set");
            if (snapshotMode == null) throw new IllegalStateException("SnapshotMode not set");
            if (snapshotStrategy == null && snapshotMode != SnapshotMode.DISABLED) {
                if (snapshotConsumerSupplier == null)
                    throw new IllegalStateException("SnapshotConsumerSupplier not set");
            }
            return new DefaultSubscriptionsHandler<>(this);
        }
    }

    static class DefaultSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        private final ConnectionSpec<K, V> configSpec;
        private final MetadataListener metadataListener;
        private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;

        private final Logger logger;
        private final RecordMapper<K, V> recordMapper;
        private final ExecutorService pool;
        private final SnapshotDeliveryStrategy<K, V> snapshotStrategy;

        private final SubscribedItems subscribedItems;
        private final ReentrantLock consumerLock = new ReentrantLock();

        private KafkaConsumerWrapper<K, V> consumer; // guarded by consumerLock
        private FutureStatus futureStatus; // guarded by consumerLock
        private int itemsCount; // guarded by consumerLock

        private EventListener eventListener;

        // Only for testing purposes: hook invoked before acquiring lock in
        // decrementAndMaybeStopConsuming()
        Runnable stopConsumingHook = () -> {};

        DefaultSubscriptionsHandler(Builder<K, V> builder) {
            this.configSpec = builder.connectionSpec;
            this.metadataListener = builder.metadataListener;
            this.consumerSupplier = builder.consumerSupplier;
            this.logger = LogFactory.getLogger(configSpec.connectionName());
            this.recordMapper =
                    RecordMapper.from(configSpec.itemTemplates(), configSpec.fieldsExtractor());
            this.snapshotStrategy =
                    builder.snapshotStrategy != null
                            ? builder.snapshotStrategy
                            : resolveStrategy(
                                    builder.snapshotMode,
                                    recordMapper,
                                    builder.snapshotConsumerSupplier,
                                    configSpec);
            this.pool =
                    Executors.newSingleThreadExecutor(r -> new Thread(r, "SubscriptionHandler"));
            this.subscribedItems = SubscribedItems.create();
        }

        private static <K, V> SnapshotDeliveryStrategy<K, V> resolveStrategy(
                SnapshotMode mode,
                RecordMapper<K, V> recordMapper,
                Supplier<Consumer<byte[], byte[]>> snapshotConsumerSupplier,
                ConnectionSpec<K, V> spec) {
            if (mode == SnapshotMode.DISABLED) {
                return new NoSnapshotStrategy<>();
            }

            SnapshotConnectionSpec<K, V> snapshotSpec =
                    new SnapshotConnectionSpec<>() {
                        @Override
                        public String connectionName() {
                            return spec.connectionName();
                        }

                        @Override
                        public ItemTemplates<K, V> itemTemplates() {
                            return spec.itemTemplates();
                        }

                        @Override
                        public KafkaRecord.DeserializerPair<K, V> deserializerPair() {
                            return spec.deserializerPair();
                        }

                        @Override
                        public CommandModeStrategy commandModeStrategy() {
                            return spec.commandModeStrategy();
                        }
                    };

            return switch (mode) {
                case CONSUMER ->
                        new ConsumerSnapshotStrategy<>(
                                snapshotSpec, recordMapper, snapshotConsumerSupplier);

                case CACHE ->
                        new CacheSnapshotStrategy<>(
                                snapshotSpec, recordMapper, snapshotConsumerSupplier);
                case DISABLED -> throw new AssertionError("Unreachable");
            };
        }

        @Override
        public void setListener(ItemEventListener listener) {
            if (listener == null) {
                throw new IllegalArgumentException("ItemEventListener cannot be null");
            }
            this.eventListener = EventListener.smartEventListener(listener);
            snapshotStrategy.init();
        }

        @Override
        public void subscribe(String item, Object itemHandle) throws SubscriptionException {
            try {
                SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
                if (!configSpec.itemTemplates().matches(newItem)) {
                    logger.atWarn()
                            .log("Item [{}] does not match any defined item templates", item);
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }

                logger.atInfo().log("Subscribed to item [{}]", item);

                subscribedItems.addItem(newItem);

                // TODO: temporarily always enabled; gate on a snapshot.enable configuration
                // property. When disabled, call item.enableRealtimeEvents(eventListener) instead.
                incrementAndMaybeStartConsuming(newItem);
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

        private void incrementAndMaybeStartConsuming(SubscribedItem item) {
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
                snapshotStrategy.deliverSnapshot(item, eventListener);
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
                    snapshotStrategy.onAllItemsUnsubscribed();
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
                    configSpec,
                    metadataListener,
                    eventListener,
                    subscribedItems,
                    recordMapper,
                    consumerSupplier);
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
