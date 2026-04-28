
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

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Wraps a Kafka {@link Consumer} to manage its full lifecycle: connection, subscription, polling,
 * and graceful shutdown.
 *
 * <p>Instances are created in a {@link FutureStatus.State#CONNECTED CONNECTED} state and transition
 * through {@link FutureStatus.State#INITIALIZED INITIALIZED} to one of the terminal states via
 * {@link #start(ExecutorService)} and {@link #shutdown()}.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public class KafkaConsumerWrapper<K, V> {

    /**
     * Represents the asynchronous status of a {@link KafkaConsumerWrapper}.
     *
     * <p>The status wraps a {@link CompletableFuture} whose value is the current {@link State} of
     * the consumer lifecycle.
     */
    public static class FutureStatus {

        /** The lifecycle states of a {@link KafkaConsumerWrapper}. */
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

            /** The consuming loop exited normally due to shutdown. */
            LOOP_CLOSED_BY_SHUTDOWN,

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

        /**
         * Waits for the state to be determined and returns it.
         *
         * @return the resolved {@link State}
         */
        public State join() {
            return futureState.join();
        }

        /**
         * Checks whether the state has already been determined.
         *
         * @return {@code true} if the state is available, {@code false} otherwise
         */
        public boolean isStateAvailable() {
            return futureState.isDone();
        }

        /**
         * Checks if the Kafka consumer is currently connected.
         *
         * <p>This method verifies both that the connection state has been determined (future is
         * completed) and that the actual state is CONNECTED.
         *
         * @return {@code true} if the consumer is connected to Kafka, {@code false} otherwise
         */
        public boolean isConnected() {
            return futureState.isDone() && futureState.join().equals(State.CONNECTED);
        }

        /**
         * Checks whether initialization has failed.
         *
         * @return {@code true} if the consumer failed during initialization, {@code false}
         *     otherwise
         */
        public boolean initFailed() {
            return futureState.isDone() && futureState.join().initFailed();
        }

        /**
         * Checks whether the consuming loop has exited, either normally or due to an exception.
         *
         * @return {@code true} if the loop has closed, {@code false} otherwise
         */
        public boolean isClosed() {
            if (!futureState.isDone()) {
                return false;
            }
            State state = futureState.join();
            return state.equals(State.LOOP_CLOSED_BY_EXCEPTION)
                    || state.equals(State.LOOP_CLOSED_BY_SHUTDOWN);
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

        /**
         * Creates a {@code FutureStatus} in the {@link State#CONNECTED} state.
         *
         * @return a new {@code FutureStatus} representing a connected consumer
         */
        public static FutureStatus connected() {
            return new FutureStatus(CompletableFuture.completedFuture(State.CONNECTED));
        }
    }

    public enum DeserializationTiming {
        DEFERRED,
        EAGER
    }

    /**
     * Encapsulates the deserialization strategy for Kafka records polled as raw bytes.
     *
     * <p>Subclasses define whether deserialization happens eagerly (at poll time) or is deferred
     * (on first access). Use the {@link #forTiming(DeserializationTiming, DeserializerPair)}
     * factory method to obtain the appropriate implementation.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    public abstract static class RecordDeserializationMode<K, V> {

        protected final DeserializationTiming timing;
        protected final KafkaRecord.DeserializerPair<K, V> deserializerPair;

        RecordDeserializationMode(
                DeserializationTiming timing, KafkaRecord.DeserializerPair<K, V> deserializerPair) {
            this.timing = timing;
            this.deserializerPair = deserializerPair;
        }

        /**
         * Converts raw polled records into a typed {@link RecordBatch}.
         *
         * @param records the raw records returned by {@link Consumer#poll(Duration)}
         * @param joinable whether the batch should support synchronous join semantics
         * @return a new {@link RecordBatch} containing the deserialized records
         */
        public abstract RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable);

        /**
         * Converts raw polled records into a non-joinable {@link RecordBatch}.
         *
         * @param records the raw records returned by {@link Consumer#poll(Duration)}
         * @return a new {@link RecordBatch} containing the deserialized records
         */
        public RecordBatch<K, V> toBatch(ConsumerRecords<byte[], byte[]> records) {
            return toBatch(records, false);
        }

        /**
         * Returns the {@link DeserializationTiming} strategy used by this instance.
         *
         * @return the deserialization timing
         */
        public DeserializationTiming getTiming() {
            return timing;
        }

        /**
         * Creates a {@code RecordDeserializationMode} for the specified timing strategy.
         *
         * @param <K> the type of the key in the Kafka record
         * @param <V> the type of the value in the Kafka record
         * @param timing the {@link DeserializationTiming} to use
         * @param deserializerPair the {@link DeserializerPair} for key and value deserialization
         * @return a new {@code RecordDeserializationMode} instance
         */
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

    /**
     * A {@link RecordDeserializationMode} that defers deserialization until first record access.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
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

    /**
     * A {@link RecordDeserializationMode} that deserializes records eagerly at poll time.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
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

    private final ConnectionSpec<K, V> connectionSpec;
    private final MetadataListener metadataListener;
    private final Logger logger;
    private final Consumer<byte[], byte[]> consumer;
    private final OffsetService offsetService;
    private final Duration pollDuration;
    private final RecordDeserializationMode<K, V> deserializationMode;
    private final Monitor monitor;
    private final RecordConsumer<K, V> recordConsumer;
    private final boolean groupConsumer;
    private final ReentrantLock statusLock = new ReentrantLock();
    private volatile boolean closed = false;

    private volatile Thread hook;
    private volatile FutureStatus status;

    /**
     * Creates a new {@code KafkaConsumerWrapper} and establishes a connection to the Kafka broker.
     *
     * <p>The consumer is instantiated immediately via the given supplier but does not start polling
     * until {@link #start(ExecutorService)} is called.
     *
     * @param connectionSpec the {@link ConnectionSpec} defining connection and processing settings
     * @param metadataListener the {@link MetadataListener} for force-unsubscription notifications
     * @param eventListener the {@link EventListener} that receives dispatched record updates
     * @param subscribedItems the {@link SubscribedItems} registry for routing records to items
     * @param recordMapper the {@link RecordMapper} that maps raw records to canonical items
     * @param consumerFactory factory for the underlying Kafka {@link Consumer}
     * @param groupConsumer {@code true} for a group-based consumer (subscribes to topics with
     *     offset commits), {@code false} for a standalone consumer (assigns partitions manually
     *     without offset commits)
     * @throws KafkaException if the consumer cannot be instantiated
     */
    public KafkaConsumerWrapper(
            ConnectionSpec<K, V> connectionSpec,
            MetadataListener metadataListener,
            EventListener eventListener,
            SubscribedItems subscribedItems,
            RecordMapper<K, V> recordMapper,
            Function<Properties, Consumer<byte[], byte[]>> consumerFactory,
            boolean groupConsumer)
            throws KafkaException {
        this.connectionSpec = connectionSpec;
        this.metadataListener = metadataListener;
        this.logger = LogFactory.getLogger(this.connectionSpec.connectionName());
        String bootStrapServers = getProperty(BOOTSTRAP_SERVERS_CONFIG);

        logger.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

        // Instantiate the Kafka Consumer, stripping group.id for standalone consumers.
        this.consumer = consumerFactory.apply(consumerProperties(groupConsumer));
        logger.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);
        this.groupConsumer = groupConsumer;
        this.offsetService =
                groupConsumer
                        ? OffsetService.commit(consumer, logger)
                        : OffsetService.noCommit(logger);
        this.pollDuration = MAX_POLL_DURATION;
        this.deserializationMode =
                new EagerDeserializationMode<>(this.connectionSpec.deserializerPair());
        this.monitor = newMonitor();

        // Make a new instance of RecordConsumer, single-threaded or parallel on the basis of
        // the configured number of threads.
        Concurrency concurrency = this.connectionSpec.concurrency();
        this.recordConsumer =
                RecordConsumer.<K, V>recordMapper(recordMapper)
                        .subscribedItems(subscribedItems)
                        .commandMode(this.connectionSpec.commandModeStrategy())
                        .eventListener(eventListener)
                        .offsetService(offsetService)
                        .errorStrategy(this.connectionSpec.errorHandlingStrategy())
                        .logger(logger)
                        .threads(concurrency.threads())
                        .ordering(OrderStrategy.from(concurrency.orderStrategy()))
                        .preferSingleThread(true)
                        // Pass the monitor to the RecordConsumer to allow it to record relevant
                        // metrics.
                        .monitor(monitor)
                        .build();

        logger.atInfo().log("Using {} record deserialization", deserializationMode.getTiming());

        this.status = FutureStatus.connected();
    }

    private Monitor newMonitor() {
        return new KafkaConnectorMonitor(this.connectionSpec.connectionName())
                .withScrapeInterval(MONITOR_SCRAPE_INTERVAL)
                .withDataPoints(MONITOR_DATA_POINTS)
                .withLogReporter();
    }

    private String getProperty(String key) {
        return this.connectionSpec.consumerProperties().getProperty(key);
    }

    private Properties consumerProperties(boolean groupConsumer) {
        Properties props = this.connectionSpec.consumerProperties();
        if (!groupConsumer) {
            Properties standalone = new Properties();
            standalone.putAll(props);
            standalone.remove(GROUP_ID_CONFIG);
            return standalone;
        }
        return props;
    }

    /**
     * Initializes the Kafka subscription and starts the consuming loop on the given executor.
     *
     * <p>If the consumer is not in the {@link FutureStatus.State#CONNECTED CONNECTED} state, this
     * method returns the current status without taking any action. On successful initialization,
     * the consuming loop is submitted asynchronously. On failure, resources are cleaned up and a
     * failed status is returned immediately.
     *
     * @param pool the {@link ExecutorService} to run the consuming loop on
     * @return the {@link FutureStatus} representing the outcome of the start attempt
     */
    public FutureStatus start(ExecutorService pool) {
        statusLock.lock();
        try {
            if (!status.isConnected()) {
                logger.atError()
                        .log("The current consumer's state does not allow starting the loop");
                return status;
            }

            logger.atDebug().log("Starting initialization");
            State state = this.init();
            logger.atDebug().log("Initialization completed with state: {}", state);

            if (state.initFailed()) {
                // In case of failure, immediately return a failed status.
                closeConsumer();
                metadataListener.forceUnsubscriptionAll();
                return updateStatus(CompletableFuture.completedFuture(state));
            }

            return updateStatus(CompletableFuture.supplyAsync(this::run, pool));
        } finally {
            statusLock.unlock();
        }
    }

    private FutureStatus updateStatus(CompletableFuture<FutureStatus.State> stage) {
        this.status = new FutureStatus(stage);
        return status;
    }

    private State init() {
        try {
            boolean ready = groupConsumer ? subscribeToTopics() : assignPartitions();
            if (ready) {
                this.monitor.start(MONITOR_LOG_REPORTING_INTERVAL);
                return State.INITIALIZED;
            } else {
                logger.atWarn().log("Initialization failed because no topics are available");
                return State.INIT_FAILED_BY_SUBSCRIPTION;
            }
        } catch (RuntimeException e) {
            logger.atWarn().setCause(e).log("Initialization failed because of an exception");
            return State.INIT_FAILED_BY_EXCEPTION;
        }
    }

    /**
     * Subscribes to topics using the consumer group protocol (on-demand lifecycle).
     *
     * <p>Supports both regex-based and literal topic subscriptions. The {@link OffsetService} is
     * registered as the {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}.
     *
     * @return {@code true} if at least one topic was subscribed, {@code false} otherwise
     */
    boolean subscribeToTopics() {
        ItemTemplates<K, V> templates = this.connectionSpec.itemTemplates();
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
        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics(Duration.ofMillis(30000));

        // Retain from the original requests topics the available ones.
        Set<String> existingTopics = listTopics.keySet();
        logger.atDebug().log("Existing topics on Kafka: [{}]", existingTopics);
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
    }

    /**
     * Assigns all partitions of the configured topics and seeks to the beginning (eager lifecycle).
     *
     * <p>Uses manual partition assignment ({@code assign()}) without a consumer group. No rebalance
     * listener is registered. Regex-based topics are not supported in this path.
     *
     * @return {@code true} if at least one partition was assigned, {@code false} otherwise
     */
    boolean assignPartitions() {
        Set<String> topics = new HashSet<>(this.connectionSpec.itemTemplates().topics());
        logger.atInfo().log("Assigning partitions for topics [{}]", topics);

        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics(Duration.ofMillis(30000));
        Set<String> existingTopics = listTopics.keySet();
        boolean notAllPresent = topics.retainAll(existingTopics);

        if (topics.isEmpty()) {
            logger.atWarn().log("Requested topics not found");
            return false;
        }

        if (notAllPresent) {
            String loggableTopics =
                    topics.stream()
                            .map(s -> "\"%s\"".formatted(s))
                            .collect(Collectors.joining(","));
            logger.atWarn()
                    .log(
                            "Actually assigning partitions for the following existing topics [{}]",
                            loggableTopics);
        }

        List<TopicPartition> partitions =
                topics.stream()
                        .flatMap(
                                t ->
                                        listTopics.get(t).stream()
                                                .map(
                                                        pi ->
                                                                new TopicPartition(
                                                                        pi.topic(),
                                                                        pi.partition())))
                        .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        logger.atInfo().log("Assigned {} partitions, seeking to beginning", partitions.size());
        return true;
    }

    private State run() {
        // Install the shutdown hook
        installShutdownHook();
        logger.atDebug().log("Shutdown hook set");
        try {
            consumeForEver(this.recordConsumer::consumeBatch);
        } catch (WakeupException e) {
            logger.atDebug().log("Kafka Consumer woken up");
        } catch (KafkaException e) {
            metadataListener.forceUnsubscriptionAll();
            return State.LOOP_CLOSED_BY_EXCEPTION;
        } finally {
            closeConsumer();
        }
        return State.LOOP_CLOSED_BY_SHUTDOWN;
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

    void consumeForEver(java.util.function.Consumer<RecordBatch<K, V>> recordConsumer)
            throws KafkaException {
        logger.atInfo().log(
                "Starting polling forever with poll timeout of {} ms and max.poll.records {}",
                pollDuration.toMillis(),
                getProperty(MAX_POLL_RECORDS_CONFIG));
        while (!closed) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
                RecordBatch<K, V> batch = deserializationMode.toBatch(records, false);
                recordConsumer.accept(batch);
            } catch (WakeupException we) {
                // Rethrow before the KafkaException catch (WakeupException extends KafkaException)
                logger.atDebug().log("Kafka Consumer woken up during poll");
                throw we;
            } catch (KafkaException ke) {
                // Includes SerializationException (a KafkaException subclass) thrown during eager
                // deserialization: treated as fatal, causing connector shutdown
                logger.atError().setCause(ke).log("Unrecoverable exception during poll");
                throw ke;
            } catch (Exception e) {
                logger.atError().setCause(e).log("Unexpected exception during poll");
                throw new KafkaException("Unexpected exception during poll", e);
            }
        }
    }

    /**
     * Initiates a graceful shutdown of this consumer.
     *
     * <p>The behavior depends on the current state:
     *
     * <ul>
     *   <li>{@link FutureStatus.State#SHUTDOWN SHUTDOWN} — returns immediately (idempotent).
     *   <li>{@link FutureStatus.State#CONNECTED CONNECTED} — closes resources directly (never
     *       started).
     *   <li>Loop still running — wakes up the consumer and waits for the loop to exit.
     *   <li>Init failed or loop already exited — skips to hook cleanup and state transition.
     * </ul>
     *
     * @return the {@link FutureStatus} in the {@code SHUTDOWN} state
     */
    public FutureStatus shutdown() {
        statusLock.lock();
        try {
            if (status.isShutdown()) {
                // Already shut down, nothing to do
                return status;
            }

            if (status.isConnected()) {
                // Never started — no async thread to join, just close resources
                closeConsumer();
            } else if (!status.initFailed() && !status.isClosed()) {
                // Init failure and loop exit already cleaned up — only doShutdown if loop is still
                // running
                doShutdown();
            }

            if (this.hook != null) {
                logger.atDebug().log("Removing shutdown hook");
                Runtime.getRuntime().removeShutdownHook(this.hook);
                this.hook = null;
            }
            return updateStatus(CompletableFuture.completedFuture(State.SHUTDOWN));
        } finally {
            statusLock.unlock();
        }
    }

    private void doShutdown() {
        logger.atInfo().log("Shutting down Kafka consumer");
        closed = true;
        logger.atDebug().log("Waking up consumer");
        consumer.wakeup();
        logger.atDebug().log("Consumer woken up, waiting for graceful thread completion");
        status.join();
        logger.atInfo().log("Kafka consumer shut down");
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
