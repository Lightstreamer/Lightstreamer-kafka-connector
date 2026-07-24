
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
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.RecordPipeline.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.RecordDeserializationMode.DeserializationTiming;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicConfiguration;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.monitors.KafkaConnectorMonitor;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.records.RecordBatch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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
 * {@link #start(ExecutorService, java.util.function.Consumer)} and {@link #shutdown()}.
 *
 * @see ConnectionSpec
 * @see FutureStatus
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

            /** The init stage failed because the requested topics were not found on the broker. */
            INIT_FAILED_ON_MISSING_TOPICS,

            /**
             * The init stage failed because an unexpected exception propagated out of the
             * initialization cycle.
             */
            INIT_FAILED_ON_ERROR,

            /**
             * The consuming loop terminated because an unexpected exception propagated out of the
             * poll/processing cycle.
             */
            LOOP_CLOSED_ON_ERROR,

            /**
             * The consuming loop exited gracefully because a pending {@code wakeup()} unwound an
             * in-progress {@code poll()} with a {@code WakeupException}.
             */
            LOOP_CLOSED_ON_WAKEUP,

            /** The loop is in a shutdown state. */
            SHUTDOWN;

            /**
             * Checks whether the state represents an initialization failure.
             *
             * @return {@code true} if the state is {@link #INIT_FAILED_ON_MISSING_TOPICS} or {@link
             *     #INIT_FAILED_ON_ERROR}, {@code false} otherwise
             */
            public boolean initFailed() {
                return this.equals(INIT_FAILED_ON_MISSING_TOPICS)
                        || this.equals(INIT_FAILED_ON_ERROR);
            }
        }

        private final CompletableFuture<State> futureState;

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
         * completed) and that the actual state is {@link State#CONNECTED CONNECTED}.
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

    /**
     * The outcome of {@link #trySubscribe()}, indicating whether the consumer was successfully
     * bound to any records source and by which mechanism.
     */
    enum SubscriptionOutcome {
        /** No topics or partitions could be bound (e.g., none of the requested topics exist). */
        NONE,

        /** The consumer subscribed to a set of literal topic names. */
        TOPICS,

        /** The consumer subscribed to a regex pattern of topic names. */
        PATTERN,

        /** The consumer manually assigned a set of topic partitions (MANUAL mode). */
        PARTITIONS;

        /**
         * Checks whether this outcome represents a successful subscription.
         *
         * @return {@code true} if the consumer was bound to any records source, {@code false} for
         *     {@link #NONE}
         */
        boolean isSuccessful() {
            return !this.equals(NONE);
        }
    }

    // Only for testing purposes
    static final Duration MAX_POLL_DURATION = Duration.ofMillis(5000);

    // Monitoring configuration
    private static final int MONITOR_DATA_POINTS = 120;
    private static final Duration MONITOR_SCRAPE_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MONITOR_LOG_REPORTING_INTERVAL = Duration.ofSeconds(3);

    private final ConnectionSpec<K, V> connectionSpec;
    private final Logger logger;
    private final Consumer<byte[], byte[]> consumer;
    private final OffsetService offsetService;
    private final Duration pollDuration;
    private final RecordDeserializationMode<K, V> deserializationMode;
    private final Monitor monitor;
    private final RecordConsumer<K, V> recordConsumer;
    private final SubscribedItems subscribedItems;
    private final boolean eagerLifecycle;
    private final ReentrantLock statusLock = new ReentrantLock();

    // Captures the exception that terminated the poll loop, published to the loop-closed callback
    // registered in updateStatus(). Written in run() before the loop future completes normally with
    // LOOP_CLOSED_ON_ERROR, establishing a happens-before with the callback invocation.
    private volatile KafkaException pollFailureCause;

    // Both accesses of hook (installation in start(), removal in shutdown()) are serialized by
    // statusLock, so no volatile is required for cross-thread visibility.
    private Thread hook;
    // Volatile publishes the latest lifecycle future to the shutdown-hook thread, which reads it
    // without taking statusLock in doShutdown().
    private volatile FutureStatus status;

    /**
     * Creates a new {@code KafkaConsumerWrapper} and establishes a connection to the Kafka broker.
     *
     * <p>The consumer is instantiated immediately via the given supplier but does not start polling
     * until {@link #start(ExecutorService, java.util.function.Consumer)} is called.
     *
     * @param connectionSpec the {@link ConnectionSpec} defining connection and processing settings
     * @param eventListener the {@link ItemEventListener} that receives dispatched record updates
     * @param subscribedItems the {@link SubscribedItems} registry for routing records to items and
     *     broadcasting end-of-snapshot at catch-up completion
     * @param consumerFactory factory for the underlying Kafka {@link Consumer}
     * @param eagerLifecycle {@code true} for an eager consumer (seeks assigned partitions to
     *     beginning, performs catch-up, then tails), {@code false} for an on-demand consumer
     *     (offset semantics follow the configured {@code consumer.mode}: in {@code GROUP} mode
     *     resumes from committed offsets; in {@code MANUAL} mode seeks to {@code
     *     record.consume.from} on every startup)
     * @throws KafkaException if the consumer cannot be instantiated
     */
    public KafkaConsumerWrapper(
            ConnectionSpec<K, V> connectionSpec,
            ItemEventListener eventListener,
            SubscribedItems subscribedItems,
            Function<Properties, Consumer<byte[], byte[]>> consumerFactory,
            boolean eagerLifecycle)
            throws KafkaException {
        this.connectionSpec = connectionSpec;
        this.subscribedItems = subscribedItems;
        this.logger = LogFactory.getLogger(this.connectionSpec.connectionName());
        String bootStrapServers = getProperty(BOOTSTRAP_SERVERS_CONFIG);

        logger.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

        this.consumer = consumerFactory.apply(this.connectionSpec.consumerProperties());
        logger.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);
        this.eagerLifecycle = eagerLifecycle;
        OffsetService os =
                this.connectionSpec.isManual()
                        ? OffsetService.noCommit(
                                consumer, logger, connectionSpec.recordConsumeFrom())
                        : OffsetService.commit(consumer, logger);
        this.offsetService =
                eagerLifecycle ? OffsetService.seekingCommit(os, consumer, logger) : os;
        this.pollDuration = MAX_POLL_DURATION;
        this.deserializationMode =
                RecordDeserializationMode.forTiming(
                        RecordDeserializationMode.DeserializationTiming.EAGER,
                        this.connectionSpec.deserializerPair(),
                        logger);
        this.monitor = newMonitor();

        // Make a new instance of RecordConsumer, single-threaded or parallel on the basis of
        // the configured number of threads.
        Concurrency concurrency = this.connectionSpec.pipeline().concurrency();
        this.recordConsumer =
                RecordConsumer.<K, V>recordMapper(
                                RecordMapper.from(
                                        connectionSpec.pipeline().itemTemplates(),
                                        connectionSpec.pipeline().fieldsExtractor()))
                        .subscribedItems(subscribedItems)
                        .eventListener(eventListener)
                        .offsetService(offsetService)
                        .logger(logger)
                        .errorStrategy(this.connectionSpec.pipeline().errorHandlingStrategy())
                        .commandModeEnabled(this.connectionSpec.pipeline().processAsCommand())
                        .catchUpEnabled(eagerLifecycle)
                        .threads(concurrency.threads())
                        .orderStrategy(OrderStrategy.from(concurrency.orderStrategy()))
                        .singleThreadPreferred(true)
                        // Pass the monitor to the RecordConsumer to allow it to record relevant
                        // metrics.
                        .monitor(monitor)
                        .build();

        logger.atInfo().log("Using {} record deserialization", deserializationMode.getTiming());

        this.status = FutureStatus.connected();
    }

    private Monitor newMonitor() {
        return new KafkaConnectorMonitor(connectionSpec.connectionName())
                .withScrapeInterval(MONITOR_SCRAPE_INTERVAL)
                .withDataPoints(MONITOR_DATA_POINTS)
                .withLogReporter();
    }

    private String getProperty(String key) {
        return connectionSpec.consumerProperties().getProperty(key);
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
     * @param onLoopClosedByException callback invoked with the {@link KafkaException} that
     *     terminated the consuming loop when the loop closes in the {@link
     *     FutureStatus.State#LOOP_CLOSED_ON_ERROR LOOP_CLOSED_ON_ERROR} state
     * @return the {@link FutureStatus} representing the outcome of the start attempt
     */
    public FutureStatus start(
            ExecutorService pool, java.util.function.Consumer<Throwable> onLoopClosedByException) {
        statusLock.lock();
        try {
            if (!status.isConnected()) {
                logger.atError()
                        .log(
                                "The current consumer's internal state does not allow starting the loop");
                return status;
            }

            logger.atInfo().log("Starting initialization");
            State state = init();
            logger.atInfo().log("Initialization completed with state: {}", state);

            if (state.initFailed()) {
                // In case of failure, immediately return a failed status.
                cleanUpResources();
                return updateStatus(CompletableFuture.completedFuture(state));
            }

            installShutdownHook();
            return updateStatus(
                    CompletableFuture.supplyAsync(this::run, pool), onLoopClosedByException);
        } finally {
            statusLock.unlock();
        }
    }

    private FutureStatus updateStatus(
            CompletableFuture<FutureStatus.State> stage,
            java.util.function.Consumer<Throwable> onLoopClosedByException) {
        CompletableFuture<State> whenComplete =
                stage.whenComplete(
                        (state, t) -> {
                            if (state == State.LOOP_CLOSED_ON_ERROR) {
                                onLoopClosedByException.accept(pollFailureCause);
                            }
                        });
        return updateStatus(whenComplete);
    }

    private FutureStatus updateStatus(CompletableFuture<FutureStatus.State> stage) {
        status = new FutureStatus(stage);
        return status;
    }

    private State init() {
        try {
            SubscriptionOutcome subscription = trySubscribe();
            if (subscription.isSuccessful()) {
                if (eagerLifecycle) {
                    catchUp();
                }
                monitor.start(MONITOR_LOG_REPORTING_INTERVAL);
                return State.INITIALIZED;
            } else {
                logger.atWarn()
                        .log(
                                "Initialization failed because the requested topics were not found on the broker");
                return State.INIT_FAILED_ON_MISSING_TOPICS;
            }
        } catch (RuntimeException e) {
            logger.atWarn().setCause(e).log("Initialization failed because of an exception");
            return State.INIT_FAILED_ON_ERROR;
        }
    }

    /**
     * Attaches the consumer to the configured topics, dispatching to the mechanism selected by the
     * {@link ConnectionSpec}: consumer-group subscription (GROUP mode) or direct partition
     * assignment (MANUAL mode).
     *
     * <p>In GROUP mode, supports both regex-based and literal topic subscriptions, and the {@link
     * OffsetService} is registered as the {@link
     * org.apache.kafka.clients.consumer.ConsumerRebalanceListener}.
     *
     * @return the {@link SubscriptionOutcome} describing how the consumer was attached, or {@link
     *     SubscriptionOutcome#NONE} if no topics were available
     */
    SubscriptionOutcome trySubscribe() {
        if (connectionSpec.isManual()) {
            return assignManually();
        }
        return subscribeToGroup();
    }

    /**
     * Assigns partitions directly without joining a consumer group. All partitions for the
     * requested topics are manually assigned to this consumer.
     *
     * <p>Regex-based topic matching is not supported in MANUAL mode.
     */
    private SubscriptionOutcome assignManually() {
        ItemTemplates<K, V> templates = connectionSpec.pipeline().itemTemplates();
        Set<TopicConfiguration> topicConfigurations = templates.topicConfigurations();
        List<TopicPartition> assignedPartitions = new ArrayList<>();
        for (TopicConfiguration topicConfig : topicConfigurations) {
            logger.atInfo().log("Checking existing partitions for topic [{}]", topicConfig.topic());
            String topic = topicConfig.topic();

            // Fetch the partitions for the topic from the broker, with a 30-second timeout.
            List<PartitionInfo> partitionsInfo =
                    consumer.partitionsFor(topic, Duration.ofMillis(30000));
            // Per the KafkaConsumer.partitionsFor contract, an empty list means the topic is
            // not present on the broker.
            if (partitionsInfo.isEmpty()) {
                logger.atWarn().log("Topic [{}] not found on the broker; skipping", topic);
                continue;
            }

            logger.atInfo().log(
                    "Found partitions {} for topic [{}] on the broker",
                    partitionsInfo.stream().map(PartitionInfo::partition).toList(),
                    topic);

            // When specific partitions were requested, warn about any that do not exist on the
            // broker.
            Set<Integer> requestedPartitions = topicConfig.partitions();
            if (!requestedPartitions.isEmpty()) {
                Set<Integer> availablePartitions =
                        partitionsInfo.stream()
                                .map(PartitionInfo::partition)
                                .collect(Collectors.toSet());
                LinkedHashSet<Integer> missing = new LinkedHashSet<>(requestedPartitions);
                missing.removeAll(availablePartitions);
                if (!missing.isEmpty()) {
                    logger.atWarn()
                            .log(
                                    "Requested partitions {} for topic [{}] are not present on the broker; skipping",
                                    missing,
                                    topic);
                }
            } else {
                logger.atInfo().log(
                        "No specific partitions requested for topic [{}]; assigning all available partitions",
                        topic);
            }

            // If no partitions were specified, assign all available partitions for the topic.
            partitionsInfo.stream()
                    .filter(
                            pi ->
                                    requestedPartitions.isEmpty()
                                            || requestedPartitions.contains(pi.partition()))
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .forEach(assignedPartitions::add);
        }

        if (assignedPartitions.isEmpty()) {
            logger.atWarn().log("No partitions found for requested topics");
            return SubscriptionOutcome.NONE;
        }

        consumer.assign(assignedPartitions);
        logger.atInfo().log("Assigned partitions {}", assignedPartitions);
        offsetService.onPartitionsAssigned(assignedPartitions);
        return SubscriptionOutcome.PARTITIONS;
    }

    /**
     * Subscribes the consumer to the configured topics or topic pattern, letting the broker assign
     * partitions through the standard consumer-group protocol.
     *
     * <p>When a regex pattern is configured, subscribes to the pattern directly. Otherwise resolves
     * the requested topic set against the broker's currently available topics and subscribes to the
     * intersection, warning if some requested topics are missing.
     */
    private SubscriptionOutcome subscribeToGroup() {
        ItemTemplates<K, V> templates = connectionSpec.pipeline().itemTemplates();
        if (templates.isRegexEnabled()) {
            Pattern pattern = templates.subscriptionPattern().get();
            logger.atDebug().log("Subscribing to the requested pattern {}", pattern.pattern());
            consumer.subscribe(pattern, offsetService);
            return SubscriptionOutcome.PATTERN;
        }
        // Original requested topics.
        Set<String> topics = templates.topicNames();
        logger.atInfo().log("Subscribing to requested topics [{}]", topics);
        logger.atDebug().log("Checking existing topics on Kafka");

        // Check the actual available topics on Kafka.
        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics(Duration.ofMillis(30000));

        // Retain from the original requests topics the available ones.
        Set<String> existingTopics = listTopics.keySet();
        logger.atDebug().log("Existing topics on Kafka: [{}]", existingTopics);
        boolean notAllPresent = topics.retainAll(existingTopics);

        // Can't subscribe at all.
        if (topics.isEmpty()) {
            logger.atWarn().log("Requested topics not found");
            return SubscriptionOutcome.NONE;
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
        return SubscriptionOutcome.TOPICS;
    }

    /**
     * Polls until the consumer position reaches or exceeds the end offsets captured during the
     * initial partition assignment.
     *
     * <p>This method blocks the calling thread, ensuring the full topic state is materialized
     * before the adapter signals readiness. Records polled during catch-up are processed through
     * the standard {@link RecordConsumer} pipeline. The first poll triggers a rebalance which
     * causes the {@link OffsetService} to seek partitions to the beginning and capture end offsets.
     *
     * <p><strong>Note:</strong> No shutdown hook is installed during catch-up. If SIGTERM arrives
     * while this method is executing, the JVM halts abruptly without a graceful consumer close.
     * This is acceptable because no clients are connected yet and the consumer always restarts from
     * the beginning regardless of committed offsets.
     */
    void catchUp() {
        logger.atInfo().log("Starting catch-up phase until end offsets are reached");
        Map<TopicPartition, Long> endOffsets = null;
        long totalCaughtUpRecords = 0L;
        long startTime = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
            if (!records.isEmpty()) {
                RecordBatch<K, V> batch = deserializationMode.toBatch(records);
                recordConsumer.consumeBatch(batch);
                totalCaughtUpRecords += batch.count();
            }
            // End offsets are captured once the first poll triggers the rebalance callback
            if (endOffsets == null) {
                endOffsets = offsetService.getCatchUpEndOffsets();
            }
            if (endOffsets != null && hasReachedEndOffsets(endOffsets)) {
                recordConsumer.endCatchUp();
                long endTime = System.currentTimeMillis();
                logger.atInfo().log(
                        "Catch-up phase completed, total records caught up: {}, total subscriptions forced: {}, duration: {} ms",
                        totalCaughtUpRecords,
                        subscribedItems.size(),
                        endTime - startTime);
                return;
            }
        }
    }

    private boolean hasReachedEndOffsets(Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (consumer.position(entry.getKey()) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private State run() {
        try {
            consumeForEver(recordConsumer::consumeBatch);
        } catch (WakeupException e) {
            logger.atDebug().log("Internal Kafka client woken up");
        } catch (KafkaException e) {
            this.pollFailureCause = e;
            return State.LOOP_CLOSED_ON_ERROR;
        } finally {
            cleanUpResources();
        }
        return State.LOOP_CLOSED_ON_WAKEUP;
    }

    private void installShutdownHook() {
        this.hook =
                new Thread(
                        () -> {
                            logger.atInfo().log("Invoked shutdown hook");
                            doShutdown();
                        },
                        "KafkaConnector Shutdown Hook");
        Runtime.getRuntime().addShutdownHook(hook);
    }

    /**
     * Runs the main polling loop until interrupted by a wakeup or terminated by an unrecoverable
     * exception. Each iteration polls the internal Kafka consumer, deserializes the returned
     * records into a {@link RecordBatch}, and forwards the batch to the given consumer function.
     *
     * <p>Exception handling:
     *
     * <ul>
     *   <li>{@link WakeupException} — rethrown so the caller can distinguish graceful shutdown from
     *       a fatal error.
     *   <li>{@link KafkaException} — rethrown as an unrecoverable error (includes {@code
     *       SerializationException} raised during eager deserialization).
     *   <li>Any other exception — wrapped in a new {@code KafkaException} and rethrown.
     * </ul>
     *
     * @param recordConsumer the function that consumes each polled {@code RecordBatch}
     * @throws KafkaException if a non-wakeup exception terminates the polling loop
     */
    void consumeForEver(java.util.function.Consumer<RecordBatch<K, V>> recordConsumer)
            throws KafkaException {
        logger.atInfo().log(
                "Starting polling forever with poll timeout of {} ms and max.poll.records {}",
                pollDuration.toMillis(),
                getProperty(MAX_POLL_RECORDS_CONFIG));
        while (true) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
                RecordBatch<K, V> batch = deserializationMode.toBatch(records, false);
                recordConsumer.accept(batch);
            } catch (WakeupException we) {
                // Rethrow before the KafkaException catch (WakeupException extends KafkaException)
                logger.atDebug().log("Kafka consumer woken up during poll");
                throw we;
            } catch (KafkaException ke) {
                // Includes SerializationException (a KafkaException subclass) thrown during eager
                // deserialization: treated as fatal, causing connector shutdown
                logger.atError().setCause(ke).log("Unrecoverable exception during polling");
                throw ke;
            } catch (Exception e) {
                logger.atError().setCause(e).log("Unexpected exception during polling");
                throw new KafkaException("Unexpected exception during polling", e);
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
                // Never started — no async thread to join, just clean up resources
                cleanUpResources();
            } else if (!status.isStateAvailable()) {
                // Only doShutdown if the loop is still running (future not yet resolved); init
                // failure and loop exit have already cleaned up their own resources
                doShutdown();
            }

            if (hook != null) {
                logger.atDebug().log("Removing shutdown hook");
                Runtime.getRuntime().removeShutdownHook(hook);
                hook = null;
            }
            return updateStatus(CompletableFuture.completedFuture(State.SHUTDOWN));
        } finally {
            statusLock.unlock();
        }
    }

    private void doShutdown() {
        logger.atInfo().log("Shutting down Kafka consumer");
        logger.atInfo().log("Waking up internal Kafka client");
        consumer.wakeup();
        logger.atInfo().log("Waiting for graceful thread completion");
        status.join();
        logger.atInfo().log("Kafka consumer shut down");
    }

    private void cleanUpResources() {
        logger.atInfo().log("Start closing internal resources");
        recordConsumer.close();
        // Ensure that all pending offsets are committed
        offsetService.onConsumerShutdown();
        // Now it's safe to close the consumer
        consumer.close();
        // Stop the monitor
        this.monitor.stop();
        logger.atInfo().log("Internal resources closed");
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
