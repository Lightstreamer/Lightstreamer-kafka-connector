
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
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.monitors.KafkaConnectorMonitor;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;

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

    public enum DeserializationTiming {
        DEFERRED,
        EAGER
    }

    public abstract static class RecordDeserializationMode<K, V> {

        protected final KafkaRecord.DeserializerPair<K, V> deserializerPair;
        protected final DeserializationTiming timing;

        RecordDeserializationMode(
                DeserializationTiming timing, KafkaRecord.DeserializerPair<K, V> deserializerPair) {
            this.deserializerPair = deserializerPair;
            this.timing = timing;
        }

        public abstract RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable);

        public RecordBatch<K, V> toBatch(ConsumerRecords<byte[], byte[]> records) {
            return toBatch(records, false);
        }

        public DeserializationTiming getTiming() {
            return timing;
        }

        public static <K, V> RecordDeserializationMode<K, V> forTiming(
                DeserializationTiming timing, DeserializerPair<K, V> deserializerPair) {
            switch (timing) {
                case DEFERRED:
                    return new DeferredDeserializationMode<>(deserializerPair);
                case EAGER:
                    return new EagerDeserializationMode<>(deserializerPair);
                default:
                    throw new IllegalArgumentException("Unknown timing: " + timing);
            }
        }
    }

    static class DeferredDeserializationMode<K, V> extends RecordDeserializationMode<K, V> {

        DeferredDeserializationMode(KafkaRecord.DeserializerPair<K, V> deserializerPair) {
            super(DeserializationTiming.DEFERRED, deserializerPair);
        }

        @Override
        public RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable) {
            return RecordBatch.batchFromDeferred(records, deserializerPair, joinable);
        }
    }

    static class EagerDeserializationMode<K, V> extends RecordDeserializationMode<K, V> {

        EagerDeserializationMode(KafkaRecord.DeserializerPair<K, V> deserializerPair) {
            super(DeserializationTiming.EAGER, deserializerPair);
        }

        @Override
        public RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable) {
            return RecordBatch.batchFromEager(records, deserializerPair, joinable);
        }
    }

    static final Duration MAX_POLL_DURATION = Duration.ofMillis(5000);

    // Monitoring configuration
    private static final int MONITOR_DATA_POINTS = 120;
    private static final Duration MONITOR_SCRAPE_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MONITOR_LOG_REPORTING_INTERVAL = Duration.ofSeconds(3);

    private final Config<K, V> config;
    private final MetadataListener metadataListener;
    private final Logger logger;
    private final RecordMapper<K, V> recordMapper;
    private final Consumer<byte[], byte[]> consumer;
    private final OffsetService offsetService;
    private final RecordConsumer<K, V> recordConsumer;
    private final Duration pollDuration;
    private final ReentrantLock statusLock = new ReentrantLock();
    private final RecordDeserializationMode<K, V> deserializationMode;
    private final Monitor monitor;

    private volatile Thread hook;
    private volatile FutureStatus status;

    public KafkaConsumerWrapper(
            Config<K, V> config,
            MetadataListener metadataListener,
            EventListener eventListener,
            SubscribedItems subscribedItems,
            Supplier<Consumer<byte[], byte[]>> consumerSupplier)
            throws KafkaException {
        this.config = config;
        this.metadataListener = metadataListener;
        this.logger = LogFactory.getLogger(this.config.connectionName());
        this.recordMapper =
                RecordMapper.<K, V>builder()
                        .withCanonicalItemExtractors(config.itemTemplates().groupExtractors())
                        .enableRegex(config.itemTemplates().isRegexEnabled())
                        .withFieldExtractor(config.fieldsExtractor())
                        .build();
        this.pollDuration = MAX_POLL_DURATION;
        String bootStrapServers = getProperty(BOOTSTRAP_SERVERS_CONFIG);

        logger.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

        // Instantiate the Kafka Consumer
        this.consumer = consumerSupplier.get();
        logger.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);
        this.status = FutureStatus.connected();
        this.offsetService = Offsets.OffsetService(consumer, logger);

        // Initialize the monitor for this consumer wrapper instance.
        this.monitor = newMonitor();

        // Make a new instance of RecordConsumer, single-threaded or parallel on the basis of
        // the configured number of threads.
        Concurrency concurrency = this.config.concurrency();
        this.recordConsumer =
                RecordConsumer.<K, V>recordMapper(recordMapper)
                        .subscribedItems(subscribedItems)
                        .commandMode(this.config.commandModeStrategy())
                        .eventListener(eventListener)
                        .offsetService(offsetService)
                        .errorStrategy(this.config.errorHandlingStrategy())
                        .logger(logger)
                        .threads(concurrency.threads())
                        .ordering(OrderStrategy.from(concurrency.orderStrategy()))
                        .preferSingleThread(true)
                        // Pass the monitor to the RecordConsumer to allow it to record relevant
                        // metrics.
                        .monitor(monitor)
                        .build();

        KafkaRecord.DeserializerPair<K, V> deserializers =
                new KafkaRecord.DeserializerPair<>(
                        config.suppliers().keySelectorSupplier().deserializer(),
                        config.suppliers().valueSelectorSupplier().deserializer());
        this.deserializationMode = new EagerDeserializationMode<>(deserializers);
        // this.deserializationMode = new DeferredDeserializationMode<>(deserializers);
        // this.deserializationMode =
        //         recordConsumer.isParallel()
        //                 ? new DeferredDeserializationMode<>(deserializers)
        //                 : new EagerDeserializationMode<>(deserializers);

        // ((AbstractRecordConsumer<K, V>) this.recordConsumer)
        //         .setDeserializationMode(deserializationMode);
        logger.atInfo().log("Using {} record deserialization", deserializationMode.getTiming());
    }

    private Monitor newMonitor() {
        return new KafkaConnectorMonitor(this.config.connectionName())
                .withScrapeInterval(MONITOR_SCRAPE_INTERVAL)
                .withDataPoints(MONITOR_DATA_POINTS)
                .withLogReporter();
    }

    private String getProperty(String key) {
        return this.config.consumerProperties().getProperty(key);
    }

    public FutureStatus startLoop(ExecutorService pool, boolean waitForInit) {
        statusLock.lock();
        try {
            if (!status.isConnected()) {
                logger.atError()
                        .log("The current consumer's state does not allow starting the loop");
                return status;
            }

            CompletableFuture<FutureStatus.State> initStage =
                    CompletableFuture.supplyAsync(this::init, pool);
            if (waitForInit) {
                logger.atDebug().log("Blocking until initialization completes");
                State state = initStage.join();
                logger.atDebug().log("Initialization completed");
                if (state.initFailed()) {
                    // In case of failure, immediately return a failed status.
                    // This is mandatory for use cases where the initialization must be
                    // completed before starting the loop.
                    return updateStatus(initStage);
                }
            }

            return updateStatus(initStage.thenApplyAsync(this::runLoop, pool));
        } finally {
            statusLock.unlock();
        }
    }

    private FutureStatus updateStatus(CompletableFuture<FutureStatus.State> stage) {
        this.status = new FutureStatus(stage);
        return status;
    }

    private FutureStatus.State init() {
        if (!subscribed()) {
            logger.atWarn().log("Initialization failed because no topics are subscribed");
            closeConsumer();
            metadataListener.forceUnsubscriptionAll();
            return FutureStatus.State.INIT_FAILED_BY_SUBSCRIPTION;
        }

        this.monitor.start(MONITOR_LOG_REPORTING_INTERVAL);

        try {
            pollOnce(this::initStoreAndConsume);
        } catch (KafkaException e) {
            logger.atWarn().log("Initialization failed because of an exception");
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
                            logger.atInfo().log("Invoked shutdown hook");
                            doShutdown();
                        });
        Runtime.getRuntime().addShutdownHook(hook);
    }

    boolean subscribed() {
        ItemTemplates<K, V> templates = this.config.itemTemplates();
        if (templates.isRegexEnabled()) {
            Pattern pattern = templates.subscriptionPattern().get();
            logger.debug("Subscribing to the requested pattern {}", pattern.pattern());
            consumer.subscribe(pattern, offsetService);
            return true;
        }
        // Original requested topics.
        Set<String> topics = new HashSet<>(templates.topics());
        logger.atInfo().log("Subscribing to requested topics [{}]", topics);
        logger.atDebug().log("Checking existing topics on Kafka");

        // Check the actual available topics on Kafka.
        try {
            Map<String, List<PartitionInfo>> listTopics =
                    consumer.listTopics(Duration.ofMillis(30000));

            // Retain from the original requests topics the available ones.
            Set<String> existingTopics = listTopics.keySet();
            boolean notAllPresent = topics.retainAll(existingTopics);

            // Can't subscribe at all. Force unsubscription and exit the loop.
            if (topics.isEmpty()) {
                logger.atWarn().log("Requested topics not found");
                return false;
            }

            // Just warn that not all requested topics can be subscribed.
            if (notAllPresent) {
                String loggableTopics =
                        topics.stream()
                                .map(s -> "\"%s\"".formatted(s))
                                .collect(Collectors.joining(","));
                logger.atWarn()
                        .log(
                                "Actually subscribing to the following existing topics [{}]",
                                loggableTopics);
            }
            consumer.subscribe(topics, offsetService);
            return true;
        } catch (Exception e) {
            logger.atError().setCause(e).log();
            return false;
        }
    }

    void pollOnce(java.util.function.Consumer<RecordBatch<K, V>> batchConsumer)
            throws KafkaException {
        logger.atInfo().log("Starting first poll to initialize the offset store", pollDuration);

        doPoll(batchConsumer, pollDuration);
        logger.atInfo().log("First poll completed");
    }

    private void doPoll(
            java.util.function.Consumer<RecordBatch<K, V>> batchConsumer, Duration pollTimeout)
            throws KafkaException {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            RecordBatch<K, V> batch = deserializationMode.toBatch(records, false);
            batchConsumer.accept(batch);
        } catch (WakeupException we) {
            // Catch and rethrow the exception here because of the next KafkaException
            logger.atDebug().log("Kafka Consumer woke up during poll");
            throw we;
        } catch (KafkaException ke) {
            logger.atError().setCause(ke).log("Unrecoverable exception");
            metadataListener.forceUnsubscriptionAll();
            throw ke;
        } catch (Exception e) {
            logger.atError().setCause(e).log("Unexpected exception during poll");
            metadataListener.forceUnsubscriptionAll();
            throw new KafkaException("Unexpected exception during poll", e);
        }
    }

    private void closeConsumer() {
        logger.atDebug().log("Start closing Kafka Consumer");
        recordConsumer.close();
        // Ensure that all pending offsets are committed
        offsetService.onConsumerShutdown();
        // Now it's safe to close the consumer
        consumer.close();
        // Stop the monitor
        this.monitor.stop();
        logger.atDebug().log("Kafka Consumer closed");
    }

    private FutureStatus.State runLoop(State previousState) {
        if (previousState.initFailed()) {
            logger.atError().log("Failed state, no records will be consumed");
            return previousState;
        }

        // Install the shutdown hook
        installShutdownHook();
        logger.atDebug().log("Shutdown hook set");
        try {
            runPollingLoop(this.recordConsumer::consumeBatch);
        } catch (WakeupException e) {
            logger.atDebug().log("Kafka Consumer woken up");
        } catch (KafkaException e) {
            logger.atError().setCause(e).log("Unrecoverable exception during polling");
            return FutureStatus.State.LOOP_CLOSED_BY_EXCEPTION;
        } finally {
            closeConsumer();
        }
        return FutureStatus.State.LOOP_CLOSED_BY_WAKEUP;
    }

    void runPollingLoop(java.util.function.Consumer<RecordBatch<K, V>> recordConsumer)
            throws KafkaException {
        logger.atInfo().log(
                "Starting polling forever with poll timeout of {} ms and max.poll.records {}",
                pollDuration.toMillis(),
                getProperty(MAX_POLL_RECORDS_CONFIG));
        for (; ; ) {
            doPoll(recordConsumer, pollDuration);
        }
    }

    private boolean isFromLatest() {
        return getProperty(AUTO_OFFSET_RESET_CONFIG).equals("latest");
    }

    void initStoreAndConsume(RecordBatch<K, V> batch) {
        offsetService.initStore(isFromLatest());
        recordConsumer.consumeBatch(batch);
    }

    public FutureStatus shutdown() {
        statusLock.lock();
        try {
            if (status.isShutdown()) {
                return status;
            }

            doShutdown();

            if (this.hook != null) {
                logger.atDebug().log("Removing shutdown hook");
                Runtime.getRuntime().removeShutdownHook(this.hook);
                this.hook = null;
            }
            return updateStatus(CompletableFuture.completedFuture(FutureStatus.State.SHUTDOWN));
        } finally {
            statusLock.unlock();
        }
    }

    private void doShutdown() {
        logger.atInfo().log("Shutting down Kafka consumer");
        logger.atDebug().log("Waking up consumer");
        consumer.wakeup();
        logger.atDebug().log("Consumer woken up, waiting for graceful thread completion");
        status.join();
        logger.atInfo().log("Kafka consumer shut down");
    }

    // Only for testing purposes
    Consumer<byte[], byte[]> getInternalConsumer() {
        return consumer;
    }

    // Only for testing purposes
    OffsetService getOffsetService() {
        return offsetService;
    }

    // Only for testing purposes
    DeserializationTiming getRecordDeserializationTiming() {
        return deserializationMode.getTiming();
    }

    // Only for testing purposes
    RecordConsumer<K, V> getRecordConsumer() {
        return recordConsumer;
    }

    // Only for testing purposes
    Duration getPollTimeout() {
        return pollDuration;
    }

    // Only for testing purposes
    Monitor getMonitor() {
        return monitor;
    }
}
