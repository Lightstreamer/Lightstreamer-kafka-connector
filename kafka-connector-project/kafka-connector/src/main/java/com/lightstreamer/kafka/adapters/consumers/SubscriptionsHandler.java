
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
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerLoop;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.Config;
import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

    Item unsubscribe(String topic) throws SubscriptionException;

    boolean isConsuming();

    void setListener(ItemEventListener listener);

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
                        KafkaConsumerWrapper.defaultConsumerSupplier(config));
            } else {
                return new DefaultSubscriptionsHandler<>(
                        config,
                        this.metadataListener,
                        KafkaConsumerWrapper.defaultConsumerSupplier(config));
            }
        }
    }

    abstract static class AbstractSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        protected final Config<K, V> config;
        protected final MetadataListener metadataListener;
        protected final Supplier<Consumer<K, V>> consumerSupplier;

        protected final Logger log;
        private final ExecutorService pool;

        private final ConcurrentHashMap<String, SubscribedItem> sourceItems =
                new ConcurrentHashMap<>();
        protected final SubscribedItems subscribedItems;
        private final ReentrantLock loopStateLock = new ReentrantLock();
        private volatile KafkaConsumerLoop<K, V> loop;

        private volatile CompletableFuture<Void> currentFuture;
        protected ItemEventListener itemEventListener;

        AbstractSubscriptionsHandler(
                Config<K, V> config,
                MetadataListener metadataListener,
                boolean allowImplicitItems,
                Supplier<Consumer<K, V>> consumerSupplier) {
            this.config = config;
            this.metadataListener = metadataListener;
            this.consumerSupplier = consumerSupplier;
            this.log = LogFactory.getLogger(config.connectionName());
            this.pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "ConsumerTrigger"));
            this.subscribedItems = SubscribedItems.of(sourceItems.values(), allowImplicitItems);
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
                addItem(item, newItem);
                onSubscribedItem();
            } catch (ExpressionException e) {
                log.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        public final CompletableFuture<Void> startConsuming() {
            log.atTrace().log("Acquiring consumer lock to start consuming events...");
            loopStateLock.lock();
            log.atTrace().log("Lock acquired...");
            try {
                if (loop == null) {
                    log.atInfo().log("Consumer not yet initialized, creating a new one...");
                    loop = newConsumerLoop();
                    log.atInfo().log("New consumer connecting and subscribing...");
                    currentFuture = CompletableFuture.runAsync(loop, pool);
                } else {
                    log.atDebug().log("Consumer is already consuming events, nothing to do");
                }
                return currentFuture;
            } catch (KafkaException ke) {
                log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
                metadataListener.forceUnsubscriptionAll();
                return CompletableFuture.failedFuture(ke);
            } finally {
                log.atTrace().log("Releasing consumer lock...");
                loopStateLock.unlock();
                log.atTrace().log("Released consumer lock");
            }
        }

        public final Item unsubscribe(String item) throws SubscriptionException {
            Item removedItem = removeItem(item);
            if (removedItem == null) {
                throw new SubscriptionException(
                        "Unsubscribing from unexpected item [%s]".formatted(item));
            }

            onUnsubscribedItem();
            return removedItem;
        }

        public final void stopConsuming() {
            log.atTrace().log("Acquiring consumer lock to stop consuming...");
            loopStateLock.lock();
            log.atTrace().log("Lock acquired to stop consuming...");
            try {
                if (loop != null) {
                    log.atInfo().log("Stopping consumer...");
                    loop.close();
                    loop = null;
                    currentFuture = null;
                    log.atInfo().log("Consumer stopped");
                } else {
                    log.atDebug().log("Consumer is not initialized yet, nothing to do");
                }
            } finally {
                log.atTrace().log("Releasing consumer lock to stop consuming");
                loopStateLock.unlock();
                log.atTrace().log("Releases consumer lock to stop consuming");
            }
        }

        @Override
        public boolean isConsuming() {
            loopStateLock.lock();
            try {
                return loop != null;
            } finally {
                loopStateLock.unlock();
            }
        }

        final void addItem(String item, SubscribedItem newItem) {
            sourceItems.put(item, newItem);
        }

        final Item removeItem(String item) {
            return sourceItems.remove(item);
        }

        void onItemEventListenerSet() {}

        void onSubscribedItem() {}

        KafkaConsumerLoop<K, V> newConsumerLoop() throws KafkaException {
            if (itemEventListener == null) {
                throw new RuntimeException(
                        "ItemEventListener must be set before starting the consumer loop");
            }
            return new KafkaConsumerLoop<>(
                    config, metadataListener, itemEventListener, subscribedItems, consumerSupplier);
        }

        //
        SubscribedItem getSubscribedItem(String item) {
            return sourceItems.get(item);
        }

        void onUnsubscribedItem() {}
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
                boolean resetCounter = false;
                try {
                    CompletableFuture<Void> consuming = startConsuming();
                    resetCounter = consuming.isCompletedExceptionally();
                } catch (Exception e) {
                    // Actually this should never happen, as we do not expect any exception 
                    // inside startConsuming
                    resetCounter = true;
                    throw e;
                }
                if (resetCounter) {
                    itemsCounter.set(0);
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

        void onItemEventListenerSet() {
            try {
                KafkaConsumerLoop<K, V> loop = newConsumerLoop();
                log.atInfo().log("Prestarting consumer...");
                if (loop.preStart()) {
                    log.atInfo().log("Consumer prestarted");
                    startConsuming();
                } else {
                    log.atWarn().log("No subscriptions happened, consumer will not start");
                    fail("Unable to subscribe to the configured Kafka topics");
                }
            } catch (KafkaException ke) {
                log.atError().setCause(ke).log("Unable to connect to Kafka");
                fail("Unable to start consuming events from Kafka");
            }
        }

        private void fail(String message) {
            itemEventListener.failure(new RuntimeException(message));
        }
    }
}
