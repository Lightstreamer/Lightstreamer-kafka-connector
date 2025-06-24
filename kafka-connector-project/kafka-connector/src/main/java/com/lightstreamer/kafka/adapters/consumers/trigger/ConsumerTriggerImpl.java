
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

package com.lightstreamer.kafka.adapters.consumers.trigger;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

class ConsumerTriggerImpl<K, V> implements ConsumerTrigger {

    private final ConsumerTriggerConfig<K, V> config;
    private final MetadataListener metadataListener;
    private final Function<SubscribedItems, ConsumerWrapper<K, V>> consumerWrapper;
    private final Logger log;
    private final ExecutorService pool;
    private final ReentrantLock consumerLock;
    private volatile ConsumerWrapper<K, V> consumer;
    private volatile CompletableFuture<Void> currentFuture;

    ConsumerTriggerImpl(
            ConsumerTriggerConfig<K, V> config,
            MetadataListener metadataListener,
            Function<SubscribedItems, ConsumerWrapper<K, V>> consumerWrapper) {
        this.config = config;
        this.metadataListener = metadataListener;
        this.consumerWrapper = consumerWrapper;
        this.log = LogFactory.getLogger(config.connectionName());
        this.pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "ConsumerTrigger"));
        this.consumerLock = new ReentrantLock();
    }

    @Override
    public boolean isConsuming() {
        consumerLock.lock();
        log.atTrace().log("Lock acquired to check if consuming...");
        try {
            return consumer != null;
        } finally {
            log.atTrace().log("Releasing consumer lock...");
            consumerLock.unlock();
            log.atTrace().log("Released consumer lock");
        }
    }

    @Override
    public CompletableFuture<Void> startConsuming(SubscribedItems items) {
        log.atTrace().log("Acquiring consumer lock to start consuming...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired...");
        try {
            if (consumer == null) {
                log.atTrace().log("Consumer not yet started, creating a new one...");
                consumer = consumerWrapper.apply(items);
                log.atTrace().log("Starting new consumer thread...");
                currentFuture = CompletableFuture.runAsync(consumer, pool);
                log.atTrace().log("New consumer thread started {}", currentFuture);
            }
            return currentFuture;
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
            metadataListener.forceUnsubscriptionAll();
            return CompletableFuture.failedFuture(ke);
        } finally {
            log.atTrace().log("Releasing consumer lock...");
            consumerLock.unlock();
            log.atTrace().log("Released consumer lock");
        }
    }

    @Override
    public void stopConsuming() {
        log.atTrace().log("Acquiring consumer lock to stop consuming...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired to stop consuming...");
        try {
            if (consumer != null) {
                log.atDebug().log("Stopping consumer...");
                consumer.close();
                consumer = null;
                currentFuture = null;
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

    @Override
    public ConsumerTriggerConfig<K, V> config() {
        return config;
    }

    // Only for testing purposes
    CompletableFuture<Void> getCurrentFuture() {
        return currentFuture;
    }
}
