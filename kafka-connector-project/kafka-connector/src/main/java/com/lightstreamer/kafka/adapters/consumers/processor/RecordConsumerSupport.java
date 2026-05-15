
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode.DISABLED;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
import static com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy.ORDER_BY_PARTITION;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.CommandEvents.Command;
import com.lightstreamer.kafka.adapters.consumers.processor.CommandEvents.Key;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.StartBuildingProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithEventListener;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOptionals;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.monitors.metrics.Meters;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValue;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValueFormatter;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.RecordBatch;
import com.lightstreamer.kafka.common.records.RecordBatch.RecordBatchListener;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory and implementation support for {@link RecordConsumer} and its builder chain.
 *
 * <p>This class is not meant to be instantiated.
 */
public class RecordConsumerSupport {

    private RecordConsumerSupport() {}

    /**
     * Starts building a {@link RecordProcessor} from the given mapper.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param mapper the record mapper for field extraction
     * @return the first step of the processor builder
     */
    public static <K, V> StartBuildingProcessor<K, V> startBuildingProcessor(
            RecordMapper<K, V> mapper) {
        return new StartBuildingProcessorBuilderImpl<>(mapper);
    }

    private static class StartBuildingProcessorBuilderImpl<K, V>
            implements StartBuildingProcessor<K, V> {

        // Mandatory fields
        protected RecordMapper<K, V> mapper;
        protected SubscribedItems subscribed;
        protected ItemEventListener eventListener;
        protected OffsetService offsetService;
        protected Logger logger;

        protected RecordProcessor<K, V> processor;

        // Optional and defaulted fields
        protected int threads = 1;
        protected OrderStrategy orderStrategy = ORDER_BY_PARTITION;
        protected boolean preferSingleThread = false;
        protected boolean enableCatchUp = false;
        protected CommandMode commandMode = DISABLED;
        protected RecordErrorHandlingStrategy errorStrategy = IGNORE_AND_CONTINUE;
        protected Monitor monitor;

        StartBuildingProcessorBuilderImpl(RecordMapper<K, V> mapper) {
            this.mapper = Objects.requireNonNull(mapper, "RecordMapper not set");
        }

        @Override
        public WithSubscribedItems<K, V> subscribedItems(SubscribedItems subscribedItems) {
            this.subscribed = Objects.requireNonNull(subscribedItems, "SubscribedItems not set");
            return new WithSubscribedItemsImpl<>(this);
        }
    }

    private static class HasParentBuilder<K, V> {

        final StartBuildingProcessorBuilderImpl<K, V> parentBuilder;

        HasParentBuilder(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }
    }

    private static class WithSubscribedItemsImpl<K, V> extends HasParentBuilder<K, V>
            implements WithSubscribedItems<K, V> {

        WithSubscribedItemsImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            super(parentBuilder);
        }

        @Override
        public WithEventListener<K, V> eventListener(ItemEventListener listener) {
            parentBuilder.eventListener = Objects.requireNonNull(listener, "EventListener not set");
            return new WithEventListenerImpl<>(parentBuilder);
        }
    }

    private static class WithEventListenerImpl<K, V> extends HasParentBuilder<K, V>
            implements WithEventListener<K, V> {

        WithEventListenerImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            super(parentBuilder);
        }

        @Override
        public WithOffsetService<K, V> offsetService(OffsetService offsetService) {
            parentBuilder.offsetService =
                    Objects.requireNonNull(offsetService, "OffsetService not set");
            return new WithOffsetServiceImpl<>(parentBuilder);
        }
    }

    private static class WithOffsetServiceImpl<K, V> extends HasParentBuilder<K, V>
            implements WithOffsetService<K, V> {

        WithOffsetServiceImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            super(parentBuilder);
        }

        @Override
        public WithOptionals<K, V> logger(Logger logger) {
            parentBuilder.logger = Objects.requireNonNull(logger, "Logger not set");
            return new WithOptionalsImpl<>(parentBuilder);
        }
    }

    private static class WithOptionalsImpl<K, V> extends HasParentBuilder<K, V>
            implements WithOptionals<K, V> {

        WithOptionalsImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            super(parentBuilder);
        }

        public WithOptionalsImpl<K, V> errorStrategy(RecordErrorHandlingStrategy strategy) {
            parentBuilder.errorStrategy = Objects.requireNonNull(strategy, "ErrorStrategy not set");
            return this;
        }

        public WithOptionalsImpl<K, V> commandMode(CommandMode strategy) {
            parentBuilder.commandMode = Objects.requireNonNull(strategy, "CommandMode not set");
            return this;
        }

        @Override
        public WithOptionals<K, V> enableCatchUp(boolean catchUp) {
            parentBuilder.enableCatchUp = catchUp;
            return this;
        }

        @Override
        public WithOptionals<K, V> threads(int threads) {
            parentBuilder.threads = threads;
            return this;
        }

        @Override
        public WithOptionals<K, V> ordering(OrderStrategy orderStrategy) {
            parentBuilder.orderStrategy =
                    Objects.requireNonNull(orderStrategy, "OrderStrategy not set");
            return this;
        }

        @Override
        public WithOptionals<K, V> preferSingleThread(boolean singleThread) {
            parentBuilder.preferSingleThread = singleThread;
            return this;
        }

        @Override
        public WithOptionals<K, V> monitor(Monitor monitor) {
            parentBuilder.monitor = Objects.requireNonNull(monitor, "Monitor not set");
            return this;
        }

        @Override
        public RecordConsumer<K, V> build() {
            if (parentBuilder.threads < 1 && parentBuilder.threads != -1) {
                throw new IllegalArgumentException("Threads number must be greater than zero");
            }

            ProcessUpdatesStrategy processUpdatesStrategy =
                    ProcessUpdatesStrategy.fromCommandMode(parentBuilder.commandMode);

            this.parentBuilder.processor =
                    new RecordProcessorImpl<>(
                            parentBuilder.mapper,
                            parentBuilder.subscribed,
                            parentBuilder.eventListener,
                            processUpdatesStrategy);

            if (parentBuilder.threads != 1
                    && !processUpdatesStrategy.type().allowConcurrentProcessing()) {
                throw new IllegalArgumentException(
                        "Command mode does not support parallel processing");
            }

            if (parentBuilder.threads == 1 && parentBuilder.preferSingleThread) {
                return new SingleThreadedRecordConsumer<>(parentBuilder);
            }
            return new ParallelRecordConsumer<>(parentBuilder);
        }
    }

    /**
     * Strategy for dispatching mapped record updates to {@link SubscribedItem} instances.
     *
     * @see DefaultUpdatesStrategy
     * @see CommandProcessUpdatesStrategy
     * @see AutoCommandModeProcessUpdatesStrategy
     */
    interface ProcessUpdatesStrategy {

        // Static factory methods

        /**
         * Creates a {@code ProcessUpdatesStrategy} corresponding to the given {@link CommandMode}.
         *
         * @param commandMode the command mode configuration
         * @return the matching strategy implementation
         */
        static ProcessUpdatesStrategy fromCommandMode(CommandMode commandMode) {
            return switch (commandMode) {
                case DISABLED -> defaultStrategy();
                case EXPLICIT -> commandStrategy();
                case AUTO -> autoCommandModeStrategy();
            };
        }

        /**
         * Creates a default strategy that dispatches updates without command mode semantics.
         *
         * @return a new {@link DefaultUpdatesStrategy}
         */
        static ProcessUpdatesStrategy defaultStrategy() {
            return new DefaultUpdatesStrategy();
        }

        /**
         * Creates a strategy that enforces command mode semantics on all updates.
         *
         * @return a new {@link CommandProcessUpdatesStrategy}
         */
        static ProcessUpdatesStrategy commandStrategy() {
            return new CommandProcessUpdatesStrategy();
        }

        /**
         * Creates a strategy that automatically detects and applies command mode semantics.
         *
         * @return a new {@link AutoCommandModeProcessUpdatesStrategy}
         */
        static ProcessUpdatesStrategy autoCommandModeStrategy() {
            return new AutoCommandModeProcessUpdatesStrategy();
        }

        // Queries / accessors

        /**
         * Extracts the event map from a mapped record.
         *
         * @param record the mapped record
         * @return the field map representing the event
         */
        default Map<String, String> getEvent(MappedRecord record) {
            return record.fieldsMap();
        }

        /**
         * Returns the logger used by this strategy.
         *
         * @return the {@link Logger}
         */
        Logger getLogger();

        /**
         * Returns the update dispatch type for this strategy.
         *
         * @return the {@link ProcessUpdatesType}
         */
        ProcessUpdatesType type();

        // Mutators

        /**
         * Processes a mapped record by extracting its event and sending updates to all routable
         * items.
         *
         * @param record the mapped record to process
         * @param routable the set of items to which updates should be dispatched
         * @param listener the event listener for delivering updates
         * @param isSnapshot {@code true} to deliver as snapshot, {@code false} for realtime
         */
        default void processUpdates(
                MappedRecord record,
                Set<SubscribedItem> routable,
                ItemEventListener listener,
                boolean isSnapshot) {
            final Map<String, String> updates = getEvent(record);
            sendUpdates(updates, routable, listener, isSnapshot);
        }

        /**
         * Sends updates to all routable items.
         *
         * @param updates the field map to dispatch
         * @param routable the set of items to which updates should be sent
         * @param listener the event listener for delivering updates
         * @param isSnapshot {@code true} to deliver as snapshot, {@code false} for realtime
         */
        void sendUpdates(
                Map<String, String> updates,
                Set<SubscribedItem> routable,
                ItemEventListener listener,
                boolean isSnapshot);

        /**
         * Sets the logger for this strategy.
         *
         * @param logger the logger to use for diagnostic output
         */
        void useLogger(Logger logger);
    }

    /**
     * Default strategy that dispatches updates to subscribed items without command mode semantics.
     */
    static class DefaultUpdatesStrategy implements ProcessUpdatesStrategy {

        private Logger logger = LoggerFactory.getLogger(ProcessUpdatesStrategy.class);

        @Override
        public void sendUpdates(
                Map<String, String> updates,
                Set<SubscribedItem> routable,
                ItemEventListener listener,
                boolean isSnapshot) {
            for (SubscribedItem sub : routable) {
                sub.sendEvent(updates, listener, isSnapshot);
            }
        }

        @Override
        public final void useLogger(Logger logger) {
            this.logger = Objects.requireNonNullElse(logger, this.logger);
        }

        @Override
        public Logger getLogger() {
            return logger;
        }

        @Override
        public ProcessUpdatesType type() {
            return ProcessUpdatesType.DEFAULT;
        }
    }

    /**
     * Strategy that automatically decorates updates with command mode semantics based on payload
     * content.
     */
    static class AutoCommandModeProcessUpdatesStrategy extends DefaultUpdatesStrategy {

        @Override
        public Map<String, String> getEvent(MappedRecord record) {
            Map<String, String> event = record.fieldsMap();
            if (record.isPayloadNull()) {
                String key = Key.KEY.lookUp(event);
                getLogger()
                        .atDebug()
                        .log("Payload is null, sending DELETE command for key: %s", key);
                return CommandEvents.deleteEvent(event);
            }

            return CommandEvents.decorate(event, Command.ADD);
        }

        @Override
        public ProcessUpdatesType type() {
            return ProcessUpdatesType.AUTO_COMMAND_MODE;
        }
    }

    /** Strategy that enforces strict command mode semantics, validating and routing commands. */
    static final class CommandProcessUpdatesStrategy extends DefaultUpdatesStrategy {

        CommandProcessUpdatesStrategy() {}

        @Override
        public void sendUpdates(
                Map<String, String> updates,
                Set<SubscribedItem> routable,
                ItemEventListener listener,
                boolean isSnapshot) {
            Optional<Command> command = checkInput(updates);
            if (command.isEmpty()) {
                getLogger()
                        .atWarn()
                        .log(
                                "Discarding record due to command mode fields not properly valued: key {} - command {}",
                                Key.KEY.lookUp(updates),
                                Key.COMMAND.lookUp(updates));
                return;
            }

            Command cmd = command.get();
            for (SubscribedItem sub : routable) {
                getLogger().atDebug().log("Enforce COMMAND mode semantic of records read");

                if (cmd.isSnapshot()) {
                    handleSnapshot(cmd, sub, listener);
                } else {
                    getLogger().atDebug().log(() -> "Sending %s command".formatted(cmd.toString()));
                    sub.sendEvent(updates, listener, sub.isSnapshot());
                }
            }
        }

        /**
         * Validates the command mode fields ({@code key} and {@code command}) of an event map.
         * Returns the parsed {@link Command} if both fields are present and semantically valid, or
         * empty if the record should be discarded.
         *
         * @param input the field map extracted from the record
         * @return the validated command, or empty if validation fails
         */
        Optional<Command> checkInput(Map<String, String> input) {
            if (input == null) {
                return Optional.empty();
            }

            // Retrieve the value of the mandatory "key" field from the input map.
            String key = CommandEvents.Key.KEY.lookUp(input);
            if (key == null || key.isBlank()) {
                return Optional.empty();
            }

            // Retrieve the value of the mandatory "command" field from the input map and
            Optional<Command> command = CommandEvents.Command.lookUp(input);
            if (command.isEmpty()) {
                return command;
            }

            Command cmd = command.get();

            // If the key is "snapshot", we expect the command to be either CS or EOS.
            if (CommandEvents.SNAPSHOT.equals(key)) {
                if (!cmd.isSnapshot()) {
                    return Optional.empty();
                }
                return command;
            }

            // If the key is not "snapshot", we expect the command to be one of ADD, DELETE, or
            // UPDATE.
            return switch (cmd) {
                case ADD, DELETE, UPDATE -> command;
                default -> Optional.empty();
            };
        }

        private void handleSnapshot(
                Command snapshot, SubscribedItem sub, ItemEventListener listener) {
            switch (snapshot) {
                case CS -> {
                    getLogger().atDebug().log("Sending clearSnapshot");
                    // updater.clearSnapshot(sub);
                    sub.setSnapshot(true);
                    sub.clearSnapshot(listener);
                }
                case EOS -> {
                    getLogger().atDebug().log("Sending endOfSnapshot");
                    // updater.endOfSnapshot(sub);
                    sub.setSnapshot(false);
                    sub.endOfSnapshot(listener);
                }
                default -> {
                    getLogger()
                            .atWarn()
                            .log(
                                    "Unexpected command for snapshot key, expected CS or EOS, got {}",
                                    snapshot);
                }
            }
        }

        @Override
        public ProcessUpdatesType type() {
            return ProcessUpdatesType.COMMAND;
        }
    }

    /**
     * Default {@link RecordProcessor} that maps records via a {@link RecordMapper} and delegates
     * update dispatch to a {@link ProcessUpdatesStrategy}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    static class RecordProcessorImpl<K, V> implements RecordProcessor<K, V> {

        protected final RecordMapper<K, V> recordMapper;
        protected final ProcessUpdatesStrategy processUpdatesStrategy;
        protected final ItemEventListener listener;
        protected final SubscribedItems subscribedItems;
        protected Logger logger = LoggerFactory.getLogger(RecordProcessorImpl.class);

        RecordProcessorImpl(
                RecordMapper<K, V> recordMapper,
                SubscribedItems subscribedItems,
                ItemEventListener listener,
                ProcessUpdatesStrategy processUpdatesStrategy) {
            this.recordMapper = recordMapper;
            this.listener = listener;
            this.processUpdatesStrategy = processUpdatesStrategy;
            this.subscribedItems = subscribedItems;
        }

        @Override
        public final void useLogger(Logger logger) {
            this.logger = Objects.requireNonNullElse(logger, this.logger);
            this.processUpdatesStrategy.useLogger(logger);
        }

        @Override
        public final void process(KafkaRecord<K, V> record, boolean isSnapshot)
                throws ValueException {
            MappedRecord mappedRecord = recordMapper.map(record);

            Set<SubscribedItem> routable = mappedRecord.route(subscribedItems);
            int size = routable.size();
            if (size > 0) {
                logger.atDebug().log("Routing record to {} items", size);
                processUpdatesStrategy.processUpdates(mappedRecord, routable, listener, isSnapshot);
            } else {
                logger.atDebug().log("No routable items found");
            }
        }

        @Override
        public ProcessUpdatesType processUpdatesType() {
            return processUpdatesStrategy.type();
        }
    }

    /**
     * Base implementation of {@link RecordConsumer} providing common lifecycle management,
     * monitoring, and error handling.
     *
     * <p>Subclasses implement {@link #consumeRecordBatch(RecordBatch)} to define how records are
     * dispatched to the {@link RecordProcessor}, and optionally override {@link #doEndCatchUp()} to
     * handle the catch-up to realtime transition.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected static final Duration DEFAULT_MONITOR_RANGE_INTERVAL = Duration.ofSeconds(30);

        protected final Logger logger;
        protected final RecordErrorHandlingStrategy errorStrategy;
        protected final boolean enableCatchUp;
        protected final Monitor monitor;
        protected final Meters.Counter receivedRecordCounter;
        protected final Meters.Counter processedRecordCounter;
        protected final RecordBatchListener recordBatchListener;
        protected volatile Throwable firstFailure = null;

        private final OffsetService offsetService;
        private final RecordProcessor<K, V> recordProcessor;
        private volatile boolean closed = false;

        AbstractRecordConsumer(StartBuildingProcessorBuilderImpl<K, V> builder) {
            this.offsetService = builder.offsetService;
            this.recordProcessor = builder.processor;
            this.logger = builder.logger;
            this.errorStrategy = builder.errorStrategy;
            this.enableCatchUp = builder.enableCatchUp;
            this.monitor = builder.monitor;

            // Enforce usage of the same logger
            this.recordProcessor.useLogger(logger);

            this.receivedRecordCounter =
                    new Meters.Counter(
                            "Received record", "Counts the number of received records", "msg");
            this.processedRecordCounter =
                    new Meters.Counter(
                            "Processed record", "Counts the number of processed records", "msg");
            this.recordBatchListener =
                    recordBatch -> processedRecordCounter.increment(recordBatch.count());
            configureMonitor();
        }

        void configureMonitor() {
            if (monitor != null) {
                monitor.observe(receivedRecordCounter)
                        .enableIrate()
                        .enableRate()
                        .withRangeInterval(DEFAULT_MONITOR_RANGE_INTERVAL);
                monitor.observe(processedRecordCounter)
                        .enableIrate()
                        .enableRate()
                        .withRangeInterval(DEFAULT_MONITOR_RANGE_INTERVAL);
            }
        }

        @Override
        public final void consumeBatch(RecordBatch<K, V> batch) {
            receivedRecordCounter.increment(batch.count());
            consumeRecordBatch(batch);
            offsetService.maybeCommit();
        }

        @Override
        public final void close() {
            this.closed = true;
            onPoolsShutdown();
        }

        @Override
        public final boolean hasFailedAsynchronously() {
            return firstFailure != null;
        }

        @Override
        public final boolean isClosed() {
            return this.closed;
        }

        @Override
        public final RecordErrorHandlingStrategy errorStrategy() {
            return errorStrategy;
        }

        @Override
        public final RecordProcessor<K, V> recordProcessor() {
            return recordProcessor;
        }

        @Override
        public final OffsetService offsetService() {
            return offsetService;
        }

        @Override
        public final Monitor monitor() {
            return monitor;
        }

        @Override
        public final void endCatchUp() {
            if (!enableCatchUp) {
                throw new IllegalStateException(
                        "endCatchUp() called but catch-up processing is not enabled");
            }
            doEndCatchUp();
        }

        void doEndCatchUp() {
            // Default: nothing, subclasses can override
        }

        final void saveOffsets(KafkaRecord<K, V> record) {
            offsetService.updateOffsets(record);
        }

        final void process(KafkaRecord<K, V> record, boolean isSnapshot) {
            recordProcessor.process(record, isSnapshot);
        }

        /**
         * Dispatches all records in the batch to the configured {@link RecordProcessor}.
         *
         * @param batch the batch of records to process
         */
        abstract void consumeRecordBatch(RecordBatch<K, V> batch);

        /** Hook for subclasses to shutdown additional pools before offset commit. */
        void onPoolsShutdown() {
            // Default: nothing, subclasses can override
        }

        /**
         * Records the first asynchronous failure for later propagation.
         *
         * @param t the throwable that caused the failure
         */
        final void asyncFailure(Throwable t) {
            if (firstFailure == null) {
                firstFailure = t;
            }
        }

        /**
         * Shuts down the given {@link ExecutorService}, waiting for graceful termination before
         * forcing shutdown.
         *
         * @param pool the executor service to shut down
         * @param poolName a descriptive name for logging
         */
        final void shutdownPool(ExecutorService pool, String poolName) {
            logger.atInfo().log("Shutting down {}", poolName);
            pool.shutdown();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.atWarn()
                            .log("{} did not terminate gracefully, forcing shutdown", poolName);
                    pool.shutdownNow();
                    if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
                        logger.atWarn().log("{} did not terminate after forced shutdown", poolName);
                    }
                }
            } catch (InterruptedException e) {
                logger.atWarn().log("Interrupted while waiting for {} to terminate", poolName);
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Single-threaded {@link RecordConsumer} that processes records sequentially on the calling
     * thread.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    static class SingleThreadedRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        private boolean sendAsSnapshot = false;

        SingleThreadedRecordConsumer(StartBuildingProcessorBuilderImpl<K, V> builder) {
            super(builder);
            this.sendAsSnapshot = builder.enableCatchUp;
        }

        @Override
        void consumeRecordBatch(RecordBatch<K, V> batch) {
            for (KafkaRecord<K, V> record : batch.getRecords()) {
                consumeRecord(record, sendAsSnapshot);
            }
        }

        private void consumeRecord(KafkaRecord<K, V> record, boolean isSnapshot) {
            try {
                process(record, isSnapshot);
                saveOffsets(record);
            } catch (ValueException ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy());

                switch (errorStrategy()) {
                    case IGNORE_AND_CONTINUE -> {
                        // Log the error here to catch the stack trace
                        logger.atWarn().setCause(ve).log("Ignoring error");
                        saveOffsets(record);
                    }

                    case FORCE_UNSUBSCRIPTION -> {
                        // Do not log the error here because it will fully logged from
                        // the consuming loop
                        logger.atWarn().log("Forcing unsubscription");
                        throw new KafkaException(ve);
                    }
                }
            } catch (Throwable t) {
                logger.atError().setCause(t).log("Serious error while processing record!");
                throw new KafkaException(t);
            } finally {
                record.getBatch().recordProcessed(recordBatchListener);
            }
        }

        @Override
        void doEndCatchUp() {
            this.sendAsSnapshot = false;
        }
    }

    /**
     * Multi-threaded {@link RecordConsumer} that distributes records across a fixed set of ring
     * buffers, each drained by a dedicated worker thread.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    static class ParallelRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        private static final String POOL_NAME = "Ring buffer processor pool";
        private static final int RING_BUFFER_CAPACITY = 5192;

        // Sentinel marker used by endCatchUp() to signal workers to count down the barrier.
        private static final KafkaRecord<?, ?> FLUSH_SENTINEL =
                new KafkaRecord<>() {
                    public Object key() {
                        throw new UnsupportedOperationException();
                    }

                    public Object value() {
                        throw new UnsupportedOperationException();
                    }

                    public boolean isPayloadNull() {
                        throw new UnsupportedOperationException();
                    }

                    public long timestamp() {
                        throw new UnsupportedOperationException();
                    }

                    public long offset() {
                        throw new UnsupportedOperationException();
                    }

                    public String topic() {
                        throw new UnsupportedOperationException();
                    }

                    public int partition() {
                        throw new UnsupportedOperationException();
                    }

                    public KafkaHeaders headers() {
                        throw new UnsupportedOperationException();
                    }

                    public RecordBatch<Object, Object> getBatch() {
                        throw new UnsupportedOperationException();
                    }
                };

        protected final OrderStrategy orderStrategy;
        protected final int actualThreads;

        private final BlockingQueue<KafkaRecord<K, V>>[] ringBuffers;
        private final AtomicLong roundRobinCounter = new AtomicLong(0);
        private final ExecutorService ringBufferPool;

        private volatile boolean stopping = false;
        private volatile CountDownLatch flushBarrier;

        @SuppressWarnings("unchecked")
        ParallelRecordConsumer(StartBuildingProcessorBuilderImpl<K, V> builder) {
            super(builder);
            this.orderStrategy = builder.orderStrategy;
            this.actualThreads = getActualThreadsNumber(builder.threads);

            // Initialize high-throughput ring buffers for ultra-high performance
            this.ringBuffers = new BlockingQueue[actualThreads];

            logger.atInfo().log(
                    "Initializing high-throughput mode with {} ring buffers", actualThreads);

            // Create dedicated ExecutorService for ring buffer processing
            AtomicInteger ringThreadCount = new AtomicInteger();
            this.ringBufferPool =
                    Executors.newFixedThreadPool(
                            actualThreads,
                            r -> {
                                Thread t =
                                        new Thread(
                                                r,
                                                "RingBufferProcessor-"
                                                        + ringThreadCount.getAndIncrement());
                                t.setDaemon(true);
                                return t;
                            });

            // Initialize ring buffers and start workers in realtime mode
            for (int i = 0; i < actualThreads; i++) {
                this.ringBuffers[i] = new ArrayBlockingQueue<>(RING_BUFFER_CAPACITY);
                configureMonitor(i);
                logger.atDebug().log(
                        "Initialized ring buffer {} with capacity {}", i, RING_BUFFER_CAPACITY);
                final int threadIndex = i;
                ringBufferPool.submit(() -> processRingBuffer(threadIndex, enableCatchUp));
            }
        }

        private void configureMonitor(final int threadIndex) {
            if (this.monitor == null) {
                return;
            }
            this.monitor
                    .observe(
                            new Meters.Gauge(
                                    "Ring buffer " + threadIndex + " usage",
                                    "Percentage of ring buffer capacity currently in use",
                                    () -> {
                                        int currentSize = ringBuffers[threadIndex].size();
                                        return Math.min(
                                                100.0,
                                                (currentSize * 100.0) / RING_BUFFER_CAPACITY);
                                    },
                                    "%"))
                    .enableMax()
                    .enableAverage()
                    .enableLatest(
                            2,
                            new MetricValueFormatter() {
                                @Override
                                public String formatMetric(MetricValue value) {
                                    return createUtilizationBar(value.value(), 20);
                                }
                            })
                    .withRangeInterval(DEFAULT_MONITOR_RANGE_INTERVAL);
        }

        private String createUtilizationBar(double utilizationPercent, int width) {
            int filledChars = (int) ((utilizationPercent * width) / 100);
            StringBuilder bar = new StringBuilder();

            // Different characters for different utilization levels
            char indicator;
            if (utilizationPercent > 90) {
                indicator = '█'; // High utilization - solid block
            } else if (utilizationPercent > 70) {
                indicator = '▓'; // Medium-high utilization - dark shade
            } else if (utilizationPercent > 40) {
                indicator = '▒'; // Medium utilization - medium shade
            } else {
                indicator = '░'; // Low utilization - light shade
            }

            for (int i = 0; i < width; i++) {
                if (i < filledChars) {
                    bar.append(indicator); // High utilization - solid block
                } else {
                    bar.append('·'); // Empty space
                }
            }

            return bar.toString();
        }

        private static int getActualThreadsNumber(int configuredThreads) {
            if (configuredThreads == -1) {
                return Runtime.getRuntime().availableProcessors();
            }
            return configuredThreads;
        }

        @Override
        public int numOfThreads() {
            return actualThreads;
        }

        @Override
        public boolean isParallel() {
            return true;
        }

        @Override
        public Optional<OrderStrategy> ordering() {
            return Optional.of(orderStrategy);
        }

        @Override
        void consumeRecordBatch(RecordBatch<K, V> batch) {
            if (firstFailure != null) {
                throw new KafkaException(firstFailure);
            }

            // Distribute records to ring buffers
            for (KafkaRecord<K, V> record : batch.getRecords()) {
                int bufferIndex = selectRingBuffer(record);
                try {
                    // Use blocking put to apply backpressure instead of failing
                    ringBuffers[bufferIndex].put(record);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.atWarn()
                            .log(
                                    "Interrupted while distributing records to ring buffers, stopping batch distribution early");
                    return; // Exit gracefully, allow partial batch to complete
                }
            }
        }

        @Override
        void onPoolsShutdown() {
            stopping = true;
            shutdownPool(ringBufferPool, POOL_NAME);
        }

        @Override
        @SuppressWarnings("unchecked")
        void doEndCatchUp() {
            // Send sentinel to each worker, causing them to exit their loop
            CountDownLatch barrier = new CountDownLatch(actualThreads);
            this.flushBarrier = barrier;
            for (int i = 0; i < actualThreads; i++) {
                try {
                    ringBuffers[i].put((KafkaRecord<K, V>) FLUSH_SENTINEL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            try {
                barrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Workers exited — submit new realtime tasks to the same pool
            this.flushBarrier = null;
            for (int i = 0; i < actualThreads; i++) {
                final int threadIndex = i;
                ringBufferPool.submit(() -> processRingBuffer(threadIndex, false));
            }
        }

        private int selectRingBuffer(KafkaRecord<K, V> record) {
            return switch (orderStrategy) {
                case UNORDERED ->
                        // Round-robin for maximum throughput
                        (int) (roundRobinCounter.getAndIncrement() % actualThreads);

                case ORDER_BY_PARTITION -> {
                    int hash = record.topic().hashCode();
                    hash = 31 * hash + record.partition();
                    yield Math.abs(hash) % actualThreads;
                }

                case ORDER_BY_KEY ->
                        // Hash key to maintain key ordering
                        record.key() != null
                                ? Math.abs(record.key().hashCode()) % actualThreads
                                : 0;
            };
        }

        private void processRingBuffer(int threadIndex, boolean isSnapshot) {
            final BlockingQueue<KafkaRecord<K, V>> ringBuffer = ringBuffers[threadIndex];

            logger.atDebug().log("Starting processing thread {}", threadIndex);

            while (!stopping) {
                try {
                    KafkaRecord<K, V> record = ringBuffer.poll(10, TimeUnit.MILLISECONDS);
                    if (record == null) continue;
                    if (record == FLUSH_SENTINEL) {
                        flushBarrier.countDown();
                        return; // Exit loop — flush() will submit a fresh task
                    }
                    consume(record, isSnapshot);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.atDebug().log(
                            "Processing thread {} interrupted, shutting down", threadIndex);
                    break;
                } catch (Exception e) {
                    logger.atError()
                            .setCause(e)
                            .log(
                                    "Unexpected error in ring buffer processor {}, continuing",
                                    threadIndex);
                }
            }

            // Drain remaining records after shutdown requested
            KafkaRecord<K, V> record;
            while ((record = ringBuffer.poll()) != null) {
                if (record == FLUSH_SENTINEL) continue;
                try {
                    consume(record, isSnapshot);
                } catch (Exception e) {
                    logger.atError()
                            .setCause(e)
                            .log(
                                    "Ignoring error while draining remaining records in ring buffer processor {} after shutdown",
                                    threadIndex);
                }
            }

            logger.atDebug().log("Stopped processing thread {}", threadIndex);
        }

        private void consume(KafkaRecord<K, V> record, boolean isSnapshot) {
            try {
                process(record, isSnapshot);
                saveOffsets(record);
            } catch (ValueException ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy());
                handleError(record, ve);
            } catch (Throwable t) {
                logger.atError().log("Serious error while processing record!");
                asyncFailure(t);
            } finally {
                record.getBatch().recordProcessed(recordBatchListener);
            }
        }

        private void handleError(KafkaRecord<K, V> record, ValueException ve) {
            switch (errorStrategy()) {
                case IGNORE_AND_CONTINUE -> {
                    logger.atWarn().log("Ignoring error");
                    saveOffsets(record);
                }

                case FORCE_UNSUBSCRIPTION -> {
                    logger.atWarn().log("Will force unsubscription");
                    asyncFailure(ve);
                }
            }
        }
    }
}
