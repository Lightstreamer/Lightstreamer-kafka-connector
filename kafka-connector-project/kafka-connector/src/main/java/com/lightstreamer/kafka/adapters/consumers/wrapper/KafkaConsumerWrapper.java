
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaConsumerWrapper<K, V> {

    public static class FutureStatus {

        public enum State {
            /** The loop is not started yet, no records will be consumed. */
            CONNECTED,

            /** The loop is initialized, ready to consume records. */
            INITIALIZED,

            /** The init stage failed because of subscription issues. */
            INIT_FAILED_BY_SUBSCRIPTION,

            /** The init stage failed because of an exception. */
            INIT_FAILED_BY_EXCEPTION,

            /** The loop is closed because of an exception. */
            LOOP_CLOSED_BY_EXCEPTION,

            /** The loop is closed gracefully because of a wakeup call. */
            LOOP_CLOSED_BY_WAKEUP,

            /** The loop is in a shutdown state. */
            SHUTDOWN;

            public boolean initFailed() {
                return this.equals(INIT_FAILED_BY_SUBSCRIPTION)
                        || this.equals(INIT_FAILED_BY_EXCEPTION);
            }
        }

        private CompletableFuture<State> futureState;

        private FutureStatus(CompletableFuture<State> futureState) {
            this.futureState = futureState;
        }

        public State join() {
            return futureState.join();
        }

        public boolean isStateAvailable() {
            return futureState.isDone();
        }

        /**
         * Checks if the Kafka consumer is currently connected.
         *
         * <p>This method verifies both that the connection state has been determined (future is
         * completed) and that the actual state is CONNECTED.
         *
         * @return {@code true} if the consumer is connected to Kafka, false otherwise
         */
        public boolean isConnected() {
            return futureState.isDone() && futureState.join().equals(State.CONNECTED);
        }

        public boolean initFailed() {
            return futureState.isDone() && futureState.join().initFailed();
        }

        /**
         * Checks whether this consumer has been completely shut down.
         *
         * @return {@code true} if the consumer has completed its shutdown process, {@code false} if
         *     the consumer is still active or in the process of shutting down
         */
        public boolean isShutdown() {
            return futureState.isDone() && futureState.join().equals(State.SHUTDOWN);
        }

        public static FutureStatus connected() {
            return new FutureStatus(CompletableFuture.completedFuture(State.CONNECTED));
        }
    }

    private static final Duration POLL_DURATION = Duration.ofMillis(Long.MAX_VALUE);

    private final Config<K, V> config;
    private final MetadataListener metadataListener;
    private final Logger log;
    private final RecordMapper<K, V> recordMapper;
    private final Consumer<K, V> consumer;
    private final OffsetService offsetService;
    private final RecordConsumer<K, V> recordConsumer;
    private volatile Thread hook;
    private volatile FutureStatus status;
    private ReentrantLock lock = new ReentrantLock();

    public KafkaConsumerWrapper(
            Config<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener itemEventListener,
            SubscribedItems subscribedItems,
            Supplier<Consumer<K, V>> consumerSupplier)
            throws KafkaException {
        this.config = config;
        this.metadataListener = metadataListener;
        this.log = LogFactory.getLogger(config.connectionName());
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
        this.status = FutureStatus.connected();

        Concurrency concurrency = config.concurrency();
        // Take care of holes in offset sequence only if parallel processing.
        boolean manageHoles = concurrency.isParallel();
        this.offsetService = Offsets.OffsetService(consumer, manageHoles, log);

        // Make a new instance of RecordConsumer, single-threaded or parallel on the basis of
        // the configured number of threads.
        this.recordConsumer =
                RecordConsumer.<K, V>recordMapper(recordMapper)
                        .subscribedItems(subscribedItems)
                        .commandMode(config.commandModeStrategy())
                        .eventListener(itemEventListener)
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

    public FutureStatus startLoop(ExecutorService pool, boolean waitForInit) {
        lock.lock();
        try {
            if (!status.isConnected()) {
                log.atError()
                        .log("The current consumer's state does not allow to can't start the loop");
                return status;
            }

            CompletableFuture<FutureStatus.State> initStage =
                    CompletableFuture.supplyAsync(this::init, pool);
            if (waitForInit) {
                log.atDebug().log("Blocking until initialization completes");
                State state = initStage.join();
                log.atDebug().log("Initialization completed");
                if (state.initFailed()) {
                    // In case of failure, immediately return a failed status.
                    // This is mandatory for use cases where the initialization must be
                    // completed before starting the loop.
                    return updateStatus(initStage);
                }
            }

            return updateStatus(initStage.thenApplyAsync(this::loop, pool));
        } finally {
            lock.unlock();
        }
    }

    private FutureStatus updateStatus(CompletableFuture<FutureStatus.State> stage) {
        this.status = new FutureStatus(stage);
        return status;
    }

    FutureStatus.State init() {
        if (!subscribed()) {
            log.atWarn().log("Initialization failed because no topics are subscribed");
            closeConsumer();
            metadataListener.forceUnsubscriptionAll();
            return FutureStatus.State.INIT_FAILED_BY_SUBSCRIPTION;
        }
        try {
            offsetService.initStore(isFromLatest());
            pollOnce(this::initStoreAndConsume);
            pollAvailable(this::consumeRecords);
        } catch (KafkaException e) {
            log.atWarn().log("Initialization failed because of an exception");
            closeConsumer();
            metadataListener.forceUnsubscriptionAll();
            return FutureStatus.State.INIT_FAILED_BY_EXCEPTION;
        }
        return FutureStatus.State.INITIALIZED;
    }

    private void installShutdownHook() {
        this.hook =
                new Thread(
                        () -> {
                            log.atInfo().log("Invoked shutdown hook");
                            doShutdown();
                        });
        Runtime.getRuntime().addShutdownHook(hook);
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
        try {
            Map<String, List<PartitionInfo>> listTopics =
                    consumer.listTopics(Duration.ofMillis(30000));

            // Retain from the original requests topics the available ones.
            Set<String> existingTopics = listTopics.keySet();
            boolean notAllPresent = topics.retainAll(existingTopics);

            // Can't subscribe at all. Force unsubscription and exit the loop.
            if (topics.isEmpty()) {
                log.atWarn().log("Requested topics not found");
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
            return false;
        }
    }

    void pollOnce(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer)
            throws KafkaException {
        log.atInfo().log(
                "Starting first poll to initialize the offset store and skipping the records already consumed");
        pool(recordConsumer, Duration.ofMillis(100));
        log.atInfo().log("First poll completed");
    }

    void pollAvailable(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer)
            throws KafkaException {
        log.atInfo().log("Polling all available records");
        while (pool(recordConsumer, Duration.ofMillis(100)) > 0)
            ;
        log.atInfo().log("All available records polled");
    }

    private int pool(
            java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer, Duration duration)
            throws KafkaException {
        log.atInfo().log("Polling records");
        try {
            ConsumerRecords<K, V> records = consumer.poll(duration);
            log.atDebug().log("Received records");
            recordConsumer.accept(records);
            log.atInfo().log("Consumed {} records", records.count());
            return records.count();
        } catch (WakeupException we) {
            // Catch and rethrow the exception here because of the next KafkaException
            throw we;
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unrecoverable exception");
            metadataListener.forceUnsubscriptionAll();
            throw ke;
        }
    }

    private void closeConsumer() {
        log.atDebug().log("Start closing Kafka Consumer");
        // Ensure that all pending offsets are committed
        recordConsumer.terminate();
        try {
            consumer.close();
        } catch (Exception e) {
            log.atError().setCause(e).log("Error while closing the Kafka Consumer");
        }
        log.atDebug().log("Kafka Consumer closed");
    }

    FutureStatus.State loop(State previousState) {
        if (previousState.initFailed()) {
            log.atError().log("Loop is in a failed state, no records will be consumed");
            return previousState;
        }

        // Install the shutdown hook
        installShutdownHook();
        log.atDebug().log("Shutdown hook set");
        try {
            pollForEver(this::consumeRecords);
        } catch (WakeupException e) {
            log.atDebug().log("Kafka Consumer woken up");
        } catch (KafkaException e) {
            log.atError().setCause(e).log("Unrecoverable exception during polling");
            return FutureStatus.State.LOOP_CLOSED_BY_EXCEPTION;
        } finally {
            closeConsumer();
        }
        return FutureStatus.State.LOOP_CLOSED_BY_WAKEUP;
    }

    void pollForEver(java.util.function.Consumer<ConsumerRecords<K, V>> recordConsumer)
            throws KafkaException {
        log.atInfo().log("Starting infinite pool");
        for (; ; ) {
            pool(recordConsumer, POLL_DURATION);
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

    void consumeRecords(ConsumerRecords<K, V> records) {
        recordConsumer.consumeRecords(records);
    }

    public FutureStatus shutdown() {
        lock.lock();
        try {
            if (status.isShutdown()) {
                return status;
            }
            // if (status.isConnected()
            doShutdown();
            if (this.hook != null) {
                log.atDebug().log("Removing shutdown hook");
                Runtime.getRuntime().removeShutdownHook(this.hook);
                this.hook = null;
            }
            return updateStatus(CompletableFuture.completedFuture(FutureStatus.State.SHUTDOWN));
        } finally {
            lock.unlock();
        }
    }

    private void doShutdown() {
        log.atInfo().log("Shutting down Kafka consumer");
        log.atDebug().log("Waking up consumer");
        consumer.wakeup();
        log.atDebug().log("Consumer woken up, waiting for graceful thread completion");
        status.join();
        log.atInfo().log("Kafka consumer shut down");
    }

    // Only for testing purposes
    Consumer<K, V> getInternalConsumer() {
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
}
