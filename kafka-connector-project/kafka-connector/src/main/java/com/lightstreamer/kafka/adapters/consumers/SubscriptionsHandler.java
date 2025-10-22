
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
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
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

    static Builder builder() {
        return new Builder();
    }

    void subscribe(String item, Object itemHandle) throws SubscriptionException;

    Optional<SubscribedItem> unsubscribe(String topic) throws SubscriptionException;

    boolean isConsuming();

    void setListener(ItemEventListener listener);

    default boolean consumeAtStartup() {
        return false;
    }

    boolean allowImplicitItems();

    static class Builder {

        private Config<?, ?> consumerConfig;
        private MetadataListener metadataListener;
        private boolean consumeAtStartup = true;
        private boolean allowImplicitItems = false;

        public Builder() {}

        public Builder withConsumerConfig(Config<?, ?> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }

        public Builder withMetadataListener(MetadataListener metadataListener) {
            this.metadataListener = metadataListener;
            return this;
        }

        public Builder atStartup(boolean consumeAtStartup, boolean allowImplicitItems) {
            this.consumeAtStartup = consumeAtStartup;
            this.allowImplicitItems = allowImplicitItems;
            return this;
        }

        public SubscriptionsHandler<?, ?> build() {
            return create(consumerConfig);
        }

        private <K, V> SubscriptionsHandler<K, V> create(Config<K, V> config) {
            if (this.consumeAtStartup) {
                return new AtStartupSubscriptionsHandler<>(
                        config,
                        this.metadataListener,
                        this.allowImplicitItems,
                        defaultConsumerSupplier(config));
            }
            return new DefaultSubscriptionsHandler<>(
                    config, this.metadataListener, defaultConsumerSupplier(config));
        }

        private static <K, V> Supplier<Consumer<K, V>> defaultConsumerSupplier(
                Config<K, V> config) {
            return () ->
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            config.deserializers().keyDeserializer(),
                            config.deserializers().valueDeserializer());
        }
    }

    abstract static class AbstractSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        private final Config<K, V> config;
        protected final MetadataListener metadataListener;
        private final boolean allowImplicitItems;
        private final Supplier<Consumer<K, V>> consumerSupplier;

        protected final Logger log;
        private final ExecutorService pool;

        protected final SubscribedItems subscribedItems;
        private final ReentrantLock consumerLock = new ReentrantLock();
        private volatile KafkaConsumerWrapper<K, V> consumer;

        private volatile FutureStatus futureStatus;
        protected ItemEventListener itemEventListener;

        AbstractSubscriptionsHandler(
                Config<K, V> config,
                MetadataListener metadataListener,
                boolean allowImplicitItems,
                Supplier<Consumer<K, V>> consumerSupplier) {
            this.config = config;
            this.metadataListener = metadataListener;
            this.allowImplicitItems = allowImplicitItems;
            this.consumerSupplier = consumerSupplier;
            this.log = LogFactory.getLogger(config.connectionName());
            this.pool =
                    Executors.newSingleThreadExecutor(r -> new Thread(r, "SubscriptionHandler"));
            this.subscribedItems =
                    allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create();
        }

        @Override
        public final boolean allowImplicitItems() {
            return allowImplicitItems;
        }

        @Override
        public final void setListener(ItemEventListener listener) {
            this.itemEventListener =
                    Objects.requireNonNull(listener, "ItemEventListener cannot be null");
            onItemEventListenerSet();
        }

        @Override
        public final void subscribe(String item, Object itemHandle) throws SubscriptionException {
            try {
                SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
                if (!config.itemTemplates().matches(newItem)) {
                    log.atWarn().log("Item [{}] does not match any defined item templates", item);
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }

                log.atInfo().log("Subscribed to item [{}]", item);
                subscribedItems.addItem(newItem);
                onSubscribedItem();
            } catch (ExpressionException e) {
                log.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        final FutureStatus startConsuming(boolean waitForInit) throws KafkaException {
            log.atTrace().log("Acquiring consumer lock to start consuming events...");
            consumerLock.lock();
            log.atTrace().log("Lock acquired...");
            try {
                if (consumer == null) {
                    log.atInfo().log("Consumer not yet initialized, creating a new one...");
                    consumer = newConsumer();
                    log.atInfo().log("New consumer connecting and subscribing...");
                    futureStatus = consumer.startLoop(pool, waitForInit);
                } else {
                    log.atDebug().log("Consumer is already consuming events, nothing to do");
                }
            } finally {
                log.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                log.atTrace().log("Consumer lock released");
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
            log.atTrace().log("Acquiring consumer lock to stop consuming...");
            consumerLock.lock();
            log.atTrace().log("Lock acquired to stop consuming...");
            try {
                if (consumer != null) {
                    log.atInfo().log("Stopping consumer...");
                    consumer.shutdown();
                    consumer = null;
                    futureStatus = null;
                    log.atInfo().log("Consumer stopped");
                } else {
                    log.atDebug().log("Consumer is not initialized yet, nothing to do");
                }
            } finally {
                log.atTrace().log("Releasing consumer lock to stop consuming");
                consumerLock.unlock();
                log.atTrace().log("Releases consumer lock to stop consuming");
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

        void onSubscribedItem() {}

        KafkaConsumerWrapper<K, V> newConsumer() throws KafkaException {
            if (itemEventListener == null) {
                throw new RuntimeException(
                        "ItemEventListener must be set before starting the consumer");
            }
            return new KafkaConsumerWrapper<>(
                    config, metadataListener, itemEventListener, subscribedItems, consumerSupplier);
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

        DefaultSubscriptionsHandler(
                Config<K, V> config,
                MetadataListener metadataListener,
                Supplier<Consumer<K, V>> consumerSupplier) {
            super(config, metadataListener, false, consumerSupplier);
        }

        @Override
        void onSubscribedItem() {
            if (itemsCounter.incrementAndGet() == 1) {
                try {
                    startConsuming(false);
                } catch (KafkaException ke) {
                    log.atError().setCause(ke).log("Unable to connect to Kafka");
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

        AtStartupSubscriptionsHandler(
                Config<K, V> config,
                MetadataListener metadataListener,
                boolean allowImplicitItems,
                Supplier<Consumer<K, V>> consumerSupplier) {
            super(config, metadataListener, allowImplicitItems, consumerSupplier);
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
                log.atError().setCause(ke).log("Unable to connect to Kafka");
                fail(ke);
            }
        }

        private void fail(String message) {
            fail(new RuntimeException(message));
        }

        private void fail(Throwable throwable) {
            itemEventListener.failure(throwable);
        }
    }
}
