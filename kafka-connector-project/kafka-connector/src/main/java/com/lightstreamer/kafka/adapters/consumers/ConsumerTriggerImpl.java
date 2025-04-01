
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class ConsumerTriggerImpl<K, V> implements ConsumerTrigger {

    private final ConsumerTriggerConfig<K, V> config;
    private final MetadataListener metadataListener;
    private final Function<Collection<SubscribedItem>, ConsumerWrapper<K, V>> consumerWrapper;
    private final Logger log;
    private final ExecutorService pool;
    private final ConcurrentHashMap<String, SubscribedItem> subscribedItems;
    private final AtomicInteger itemsCounter;
    private final ReentrantLock consumerLock;
    private volatile ConsumerWrapper<K, V> consumer;
    private CompletableFuture<Void> consuming;

    public ConsumerTriggerImpl(
            ConsumerTriggerConfig<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener) {
        this(
                config,
                metadataListener,
                items -> ConsumerWrapper.create(config, eventListener, metadataListener, items));
    }

    ConsumerTriggerImpl(
            ConsumerTriggerConfig<K, V> config,
            MetadataListener metadataListener,
            Function<Collection<SubscribedItem>, ConsumerWrapper<K, V>> consumerWrapper) {
        this.config = config;
        this.metadataListener = metadataListener;
        this.consumerWrapper = consumerWrapper;
        this.log = LogFactory.getLogger(config.connectionName());
        this.pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "ConsumerTrigger"));
        this.subscribedItems = new ConcurrentHashMap<>();
        this.itemsCounter = new AtomicInteger(0);
        this.consumerLock = new ReentrantLock();
    }

    public final int getItemsCounter() {
        return itemsCounter.get();
    }

    @Override
    public boolean isSnapshotAvailable(String item) {
        return config.isCommandEnforceEnabled();
    }

    @Override
    public final CompletableFuture<Void> subscribe(String item, Object itemHandle)
            throws SubscriptionException {
        try {
            SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
            if (!config.itemTemplates().matches(newItem)) {
                log.atWarn().log("Item [{}] does not match any defined item templates", item);
                throw new SubscriptionException("Item does not match any defined item templates");
            }

            log.atInfo().log("Subscribed to item [{}]", item);
            subscribedItems.put(item, newItem);
            if (itemsCounter.incrementAndGet() == 1) {
                consuming = startConsuming();
            }
            return consuming;
        } catch (ExpressionException e) {
            log.atError().setCause(e).log();
            throw new SubscriptionException(e.getMessage());
        }
    }

    CompletableFuture<Void> startConsuming() throws SubscriptionException {
        log.atTrace().log("Acquiring consumer lock...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired...");
        try {
            consumer = consumerWrapper.apply(subscribedItems.values());
            return CompletableFuture.runAsync(consumer, pool);
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
            metadataListener.forceUnsubscriptionAll();
            itemsCounter.set(0);
            return CompletableFuture.failedFuture(ke);
        } finally {
            log.atTrace().log("Releasing consumer lock...");
            consumerLock.unlock();
            log.atTrace().log("Released consumer lock");
        }
    }

    @Override
    public final Item unsubscribe(String item) throws SubscriptionException {
        Item removedItem = subscribedItems.remove(item);
        if (removedItem == null) {
            throw new SubscriptionException(
                    "Unsubscribing from unexpected item [%s]".formatted(item));
        }

        if (itemsCounter.decrementAndGet() == 0) {
            stopConsuming();
        }
        return removedItem;
    }

    void stopConsuming() {
        log.atDebug().log("No more subscribed items");
        log.atTrace().log("Acquiring consumer lock to stop consuming...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired to stop consuming...");
        try {
            if (consumer != null) {
                log.atDebug().log("Stopping consumer...");
                consumer.close();
                consumer = null;
                log.atDebug().log("Stopped consumer");
            } else {
                log.atDebug().log("Consumer not yet started");
            }
        } finally {
            log.atTrace().log("Releasing consumer lock to stop consuming");
            consumerLock.unlock();
            log.atTrace().log("Releases consumer lock to stop consuming");
        }
    }

    // Only for testing purposes
    Item getSubscribedItem(String itemName) {
        return subscribedItems.get(itemName);
    }
}
