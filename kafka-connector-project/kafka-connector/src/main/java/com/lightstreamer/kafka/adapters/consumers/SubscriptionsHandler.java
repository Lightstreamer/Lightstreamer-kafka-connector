
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
import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;
import com.lightstreamer.kafka.adapters.consumers.deserialization.DeferredDeserializer;
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
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
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

    default boolean consumeAtStartup() {
        return false;
    }

    boolean allowImplicitItems();

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class Builder<K, V> {

        private Config<K, V> consumerConfig;
        private MetadataListener metadataListener;
        private SnapshotStore snapshotStore;
        private boolean consumeAtStartup = true;
        private boolean allowImplicitItems = false;
        private Supplier<Consumer<Deferred<K>, Deferred<V>>> consumerSupplier;

        private Builder() {}

        public Builder<K, V> withConsumerConfig(Config<K, V> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }

        public Builder<K, V> withMetadataListener(MetadataListener metadataListener) {
            this.metadataListener = metadataListener;
            return this;
        }

        public Builder<K, V> withSnapshotStore(SnapshotStore snapshotStore) {
            this.snapshotStore = snapshotStore;
            return this;
        }

        public Builder<K, V> withConsumerSupplier(
                Supplier<Consumer<Deferred<K>, Deferred<V>>> consumerSupplier) {
            this.consumerSupplier = consumerSupplier;
            return this;
        }

        public Builder<K, V> atStartup(boolean consumeAtStartup, boolean allowImplicitItems) {
            this.consumeAtStartup = consumeAtStartup;
            this.allowImplicitItems = this.consumeAtStartup ? allowImplicitItems : false;
            return this;
        }

        public SubscriptionsHandler<K, V> build() {
            this.consumerSupplier =
                    Objects.requireNonNullElse(
                            this.consumerSupplier, defaultConsumerSupplier(consumerConfig));
            if (this.consumeAtStartup) {
                return new AtStartupSubscriptionsHandler<>(this);
            }
            return new DefaultSubscriptionsHandler<>(this);
        }

        private static <K, V> Supplier<Consumer<Deferred<K>, Deferred<V>>> defaultConsumerSupplier(
                Config<K, V> config) {
            return () ->
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            new DeferredDeserializer<>(
                                    config.suppliers().keySelectorSupplier().deserializer()),
                            new DeferredDeserializer<>(
                                    config.suppliers().valueSelectorSupplier().deserializer()));
        }
    }

    abstract static class AbstractSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        private final Config<K, V> config;
        protected final MetadataListener metadataListener;
        private final boolean allowImplicitItems;
        private final Supplier<Consumer<Deferred<K>, Deferred<V>>> consumerSupplier;

        protected final Logger logger;
        private final ExecutorService pool;

        protected final SubscribedItems subscribedItems;
        private final ReentrantLock consumerLock = new ReentrantLock();
        protected volatile KafkaConsumerWrapper<K, V> consumer;

        private volatile FutureStatus futureStatus;
        protected EventListener eventListener;

        AbstractSubscriptionsHandler(Builder<K, V> builder) {
            this.config = builder.consumerConfig;
            this.metadataListener = builder.metadataListener;
            this.allowImplicitItems = builder.allowImplicitItems;
            this.consumerSupplier = builder.consumerSupplier;
            this.logger = LogFactory.getLogger(config.connectionName());
            this.pool =
                    Executors.newSingleThreadExecutor(r -> new Thread(r, "SubscriptionHandler"));
            this.subscribedItems =
                    builder.allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create();
        }

        @Override
        public final boolean allowImplicitItems() {
            return allowImplicitItems;
        }

        @Override
        public final void setListener(ItemEventListener listener) {
            if (listener == null) {
                throw new IllegalArgumentException("ItemEventListener cannot be null");
            }
            this.eventListener =
                    this.subscribedItems.acceptSubscriptions()
                            ? EventListener.smartEventListener(listener)
                            : EventListener.legacyEventListener(listener);
            onItemEventListenerSet();
        }

        @Override
        public final void subscribe(String item, Object itemHandle) throws SubscriptionException {
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

                // getSnapshot... -> send snapshot updates if requested
                // newItem.sendSnapshotEvents...
                // newItem.sendSnapshotEvents...
                newItem.enableRealtimeEvents(this.eventListener);

                onSubscribedItem(newItem);
            } catch (ExpressionException e) {
                logger.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        final FutureStatus startConsuming(boolean waitForInit) throws KafkaException {
            logger.atTrace().log("Acquiring consumer lock to start consuming events...");
            consumerLock.lock();
            logger.atTrace().log("Lock acquired...");
            try {
                if (consumer == null) {
                    logger.atInfo().log("Consumer not yet initialized, creating a new one...");
                    consumer = newConsumer();
                    logger.atInfo().log("New consumer connecting and subscribing...");
                    futureStatus = consumer.startLoop(pool, waitForInit);
                } else {
                    logger.atDebug().log("Consumer is already consuming events, nothing to do");
                }
            } finally {
                logger.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                logger.atTrace().log("Consumer lock released");
            }
            return futureStatus;
        }

        @Override
        public final Optional<SubscribedItem> unsubscribe(String item) {
            Optional<SubscribedItem> removedItem = subscribedItems.removeItem(item);
            if (removedItem.isPresent()) {
                onUnsubscribedItem();
            }

            return removedItem;
        }

        final void stopConsuming() {
            logger.atTrace().log("Acquiring consumer lock to stop consuming...");
            consumerLock.lock();
            logger.atTrace().log("Lock acquired to stop consuming...");
            try {
                if (consumer != null) {
                    logger.atInfo().log("Stopping consumer...");
                    consumer.shutdown();
                    consumer = null;
                    futureStatus = null;
                    logger.atInfo().log("Consumer stopped");
                } else {
                    logger.atDebug().log("Consumer is not initialized yet, nothing to do");
                }
            } finally {
                logger.atTrace().log("Releasing consumer lock to stop consuming");
                consumerLock.unlock();
                logger.atTrace().log("Releases consumer lock to stop consuming");
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

        final void clearItems() {
            subscribedItems.clear();
        }

        void onItemEventListenerSet() {}

        void onSubscribedItem(SubscribedItem item) {}

        KafkaConsumerWrapper<K, V> newConsumer() throws KafkaException {
            if (eventListener == null) {
                throw new RuntimeException(
                        "EventListener must be set before starting the consumer");
            }
            return new KafkaConsumerWrapper<>(
                    config, metadataListener, eventListener, subscribedItems, consumerSupplier);
        }

        void onUnsubscribedItem() {}

        // Only for testing purposes
        SubscribedItems getSubscribedItems() {
            return subscribedItems;
        }

        // Only for testing purposes
        FutureStatus getFutureStatus() {
            return futureStatus;
        }
    }

    static class DefaultSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        private final AtomicInteger itemsCounter = new AtomicInteger(0);

        DefaultSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
        }

        @Override
        void onSubscribedItem(SubscribedItem item) {
            if (itemsCounter.incrementAndGet() == 1) {
                try {
                    startConsuming(false);
                    // consumer.consumeRecordsAsSnapshot(null, item);
                } catch (KafkaException ke) {
                    logger.atError().setCause(ke).log("Unable to connect to Kafka");
                    metadataListener.forceUnsubscriptionAll();
                }
            }
        }

        @Override
        void onUnsubscribedItem() {
            if (itemsCounter.decrementAndGet() == 0) {
                stopConsuming();
            }
        }

        // Only for testing purposes
        int getItemsCounter() {
            return itemsCounter.get();
        }
    }

    static class AtStartupSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        AtStartupSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
        }

        @Override
        public boolean consumeAtStartup() {
            return true;
        }

        void onItemEventListenerSet() {
            try {
                FutureStatus status = startConsuming(true);
                if (status.initFailed()) {
                    fail("Failed to start consuming from Kafka");
                }
            } catch (KafkaException ke) {
                logger.atError().setCause(ke).log("Unable to connect to Kafka");
                fail(ke);
            }
        }

        private void fail(String message) {
            fail(new RuntimeException(message));
        }

        private void fail(Exception throwable) {
            eventListener.failure(throwable);
        }
    }
}
