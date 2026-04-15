
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
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import java.util.Objects;
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
        private final ExecutorService pool;

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
            this.config = builder.consumerConfig;
            this.metadataListener = builder.metadataListener;
            this.consumerSupplier = builder.consumerSupplier;
            this.logger = LogFactory.getLogger(config.connectionName());
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
                newItem.enableRealtimeEvents(this.eventListener);

                incrementAndMaybeStartConsuming();
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
                    config, metadataListener, eventListener, subscribedItems, consumerSupplier);
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
