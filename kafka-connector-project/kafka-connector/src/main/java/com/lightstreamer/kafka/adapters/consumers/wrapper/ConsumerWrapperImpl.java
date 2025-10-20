
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class ConsumerWrapperImpl<K, V> implements ConsumerWrapper<K, V> {

    private static final Duration POLL_DURATION = Duration.ofMillis(Long.MAX_VALUE);

    private final ConsumerTriggerConfig<K, V> config;
    private final MetadataListener metadataListener;
    private final Logger log;
    private final RecordMapper<K, V> recordMapper;
    private final Consumer<K, V> consumer;
    private final OffsetService offsetService;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Function<Properties, AdminInterface> adminFactory;
    private final RecordConsumer<K, V> recordConsumer;
    private Thread hook;

    ConsumerWrapperImpl(
            ConsumerTriggerConfig<K, V> config,
            ItemEventListener eventListener,
            MetadataListener metadataListener,
            SubscribedItems subscribedItems,
            Supplier<Consumer<K, V>> consumerSupplier,
            Function<Properties, AdminInterface> admin)
            throws KafkaException {
        this.config = config;
        this.metadataListener = metadataListener;
        this.log = LogFactory.getLogger(config.connectionName());
        this.adminFactory = admin;
        this.recordMapper =
                RecordMapper.<K, V>builder()
                        .withTemplateExtractors(config.itemTemplates().groupExtractors())
                        .enableRegex(config.itemTemplates().isRegexEnabled())
                        .withFieldExtractor(config.fieldsExtractor())
                        .build();
        String bootStrapServers = getProperty(BOOTSTRAP_SERVERS_CONFIG);

        log.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

        // Instantiate the Kafka Consumer
        this.consumer = consumerSupplier.get();
        log.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);

        Concurrency concurrency = config.concurrency();

        // Take care of holes in offset sequence only if parallel processing.
        boolean manageHoles = concurrency.isParallel();
        this.offsetService = Offsets.OffsetService(consumer, manageHoles, log);

        // Make a new instance of RecordConsumer, single-threaded or parallel on the basis of
        // the configured number of threads.
        this.recordConsumer =
                RecordConsumer.<K, V>recordMapper(recordMapper)
                        .subscribedItems(subscribedItems)
                        .enforceCommandMode(config.isCommandEnforceEnabled())
                        .eventListener(eventListener)
                        .offsetService(offsetService)
                        .errorStrategy(config.errorHandlingStrategy())
                        .logger(log)
                        .threads(concurrency.threads())
                        .ordering(OrderStrategy.from(concurrency.orderStrategy()))
                        .preferSingleThread(true)
                        .build();
    }

    private String getProperty(String key) {
        return config.consumerProperties().getProperty(key);
    }

    @Override
    public void run() {
        // Install the shutdown hook
        this.hook = setShutdownHook();
        log.atDebug().log("Set shutdown hook");
        try {
            if (subscribed()) {
                pollOnce(this::initStoreAndConsume);
                pollForEver(this::consumeRecords);
            } else {
                log.atWarn().log("No subscriptions happened");
            }
        } catch (WakeupException e) {
            log.atDebug().log("Kafka Consumer woken up");
        } finally {
            log.atDebug().log("Start closing Kafka Consumer");
            recordConsumer.close();
            try {
                consumer.close();
            } catch (Exception e) {
                log.atError().setCause(e).log("Error while closing the Kafka Consumer");
            } finally {
                latch.countDown();
            }
        }
    }

    @Override
    public void close() {
        shutdown();
        if (this.hook != null) {
            Runtime.getRuntime().removeShutdownHook(this.hook);
        }
    }

    private boolean isFromLatest() {
        return getProperty(AUTO_OFFSET_RESET_CONFIG).equals("latest");
    }

    ConsumerRecords<K, V> initStoreAndConsume(ConsumerRecords<K, V> records) {
        offsetService.initStore(isFromLatest());
        // Consume all the records that don't have a pending offset, which have therefore
        // already delivered to the clients.
        return recordConsumer.consumeFilteredRecords(records, offsetService::notHasPendingOffset);
    }

    // Only for testing purposes
    Consumer<K, V> getConsumer() {
        return consumer;
    }

    // Only for testing purposes
    OffsetService getOffsetService() {
        return offsetService;
    }

    // Only for testing purposes
    RecordConsumer<K, V> getRecordConsumer() {
        return recordConsumer;
    }

    // Only for testing purposes
    RecordMapper<K, V> getRecordMapper() {
        return recordMapper;
    }

    @Override
    public void consumeRecords(ConsumerRecords<K, V> records) {
        recordConsumer.consumeRecords(records);
    }

    private Thread setShutdownHook() {
        Runnable shutdownTask =
                () -> {
                    log.atInfo().log("Invoked shutdown hook");
                    shutdown();
                };

        Thread hook = new Thread(shutdownTask);
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    protected boolean subscribed() {
        ItemTemplates<K, V> templates = config.itemTemplates();
        if (templates.isRegexEnabled()) {
            Pattern pattern = templates.subscriptionPattern().get();
            log.debug("Subscribing to the requested pattern {}", pattern.pattern());
            consumer.subscribe(pattern, offsetService);
            return true;
        }
        // Original requested topics.
        Set<String> topics = new HashSet<>(templates.topics());
        log.atInfo().log("Subscribing to requested topics [{}]", topics);
        log.atDebug().log("Checking existing topics on Kafka");

        // Check the actual available topics on Kafka.
        try (AdminInterface admin = adminFactory.apply(config.consumerProperties())) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.timeoutMs(30000);

            // Retain from the original requests topics the available ones.
            Set<String> existingTopics = admin.listTopics(options);
            boolean notAllPresent = topics.retainAll(existingTopics);

            // Can't subscribe at all. Force unsubscription and exit the loop.
            if (topics.isEmpty()) {
                log.atWarn().log("Not found requested topics");
                metadataListener.forceUnsubscriptionAll();
                return false;
            }

            // Just warn that not all requested topics can be subscribed.
            if (notAllPresent) {
                String loggableTopics =
                        topics.stream()
                                .map(s -> "\"%s\"".formatted(s))
                                .collect(Collectors.joining(","));
                log.atWarn()
                        .log(
                                "Actually subscribing to the following existing topics [{}]",
                                loggableTopics);
            }
            consumer.subscribe(topics, offsetService);
            return true;
        } catch (Exception e) {
            log.atError().setCause(e).log();
            metadataListener.forceUnsubscriptionAll();
            return false;
        }
    }

    void pollOnce(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer) {
        log.atInfo().log(
                "Starting first poll to initialize the offset store and skipping the records already consumed");
        doPoll(recordConsumer);
        log.atInfo().log("First poll completed");
    }

    void pollForEver(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer) {
        log.atInfo().log("Starting polling forever");
        for (; ; ) {
            doPoll(recordConsumer);
        }
    }

    private void doPoll(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer) {
        try {
            ConsumerRecords<K, V> records = consumer.poll(POLL_DURATION);
            log.atDebug().log("Received records");
            recordConsumer.accept(records);
            log.atDebug().log("Consumed {} records", records.count());
        } catch (WakeupException we) {
            // Catch and rethrow the exception here because of the next KafkaException
            throw we;
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unrecoverable exception");
            metadataListener.forceUnsubscriptionAll();
            throw ke;
        }
    }

    private void shutdown() {
        log.atInfo().log("Shutting down Kafka consumer");
        log.atDebug().log("Waking up consumer");
        consumer.wakeup();
        log.atDebug().log("Consumer woken up");
        try {
            log.atTrace().log("Waiting for graceful thread completion");
            latch.await();
            log.atTrace().log("Completed thread");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.atInfo().log("Shut down Kafka consumer");
    }
}
