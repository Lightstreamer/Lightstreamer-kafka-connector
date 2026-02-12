
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.StartBuildingConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.StartBuildingProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithEnforceCommandMode;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithLogger;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOptionals;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithSubscribedItems;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandMode.Command;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandMode.Key;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.monitors.metrics.Meters;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.RecordBatch;
import com.lightstreamer.kafka.common.records.RecordBatch.RecordBatchListener;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordConsumerSupport {

    public static <K, V> StartBuildingProcessor<K, V> startBuildingProcessor(
            RecordMapper<K, V> mapper) {
        return new StartBuildingProcessorBuilderImpl<>(mapper);
    }

    public static <K, V> StartBuildingConsumer<K, V> startBuildingConsumer(
            RecordProcessor<K, V> recordProcessor) {
        return new StartBuildingConsumerImpl<>(recordProcessor);
    }

    private static class StartBuildingConsumerImpl<K, V> implements StartBuildingConsumer<K, V> {

        protected RecordProcessor<K, V> processor;
        protected OffsetService offsetService;
        protected RecordErrorHandlingStrategy errorStrategy;
        protected Logger logger;
        protected Monitor monitor;

        // Optional and defaulted fields
        protected int threads = 1;
        protected OrderStrategy orderStrategy = OrderStrategy.ORDER_BY_PARTITION;
        protected boolean preferSingleThread = false;

        StartBuildingConsumerImpl(RecordProcessor<K, V> processor) {
            this.processor = Objects.requireNonNull(processor, "RecordProcessor not set");
        }

        @Override
        public WithOffsetService<K, V> offsetService(OffsetService offsetService) {
            this.offsetService = Objects.requireNonNull(offsetService, "OffsetService not set");
            return new WithOffsetServiceImpl<>(this);
        }
    }

    private static class StartBuildingProcessorBuilderImpl<K, V>
            implements StartBuildingProcessor<K, V> {

        protected RecordMapper<K, V> mapper;
        protected SubscribedItems subscribed;
        protected EventListener listener;
        protected CommandModeStrategy commandModeStrategy;

        StartBuildingProcessorBuilderImpl(RecordMapper<K, V> mapper) {
            this.mapper = Objects.requireNonNull(mapper, "RecordMapper not set");
        }

        @Override
        public WithSubscribedItems<K, V> subscribedItems(SubscribedItems subscribedItems) {
            this.subscribed = Objects.requireNonNull(subscribedItems, "SubscribedItems not set");
            return new WithSubscribedItemsImpl<>(this);
        }
    }

    private static class WithSubscribedItemsImpl<K, V> implements WithSubscribedItems<K, V> {
        final StartBuildingProcessorBuilderImpl<K, V> parentBuilder;

        WithSubscribedItemsImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public WithEnforceCommandMode<K, V> commandMode(CommandModeStrategy strategy) {
            this.parentBuilder.commandModeStrategy =
                    Objects.requireNonNull(strategy, "CommandModeStrategy not set");
            return new WithEnforceCommandModeImpl<>(parentBuilder);
        }
    }

    private static class WithEnforceCommandModeImpl<K, V> implements WithEnforceCommandMode<K, V> {
        final StartBuildingProcessorBuilderImpl<K, V> parentBuilder;

        WithEnforceCommandModeImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public StartBuildingConsumer<K, V> eventListener(EventListener listener) {
            parentBuilder.listener = Objects.requireNonNull(listener, "EventListener not set");

            ProcessUpdatesStrategy processUpdatesStrategy =
                    ProcessUpdatesStrategy.fromCommandModeStrategy(
                            parentBuilder.commandModeStrategy);

            return new StartBuildingConsumerImpl<>(
                    new DefaultRecordProcessor<>(
                            parentBuilder.mapper,
                            parentBuilder.subscribed,
                            parentBuilder.listener,
                            processUpdatesStrategy));
        }
    }

    private static class WithOffsetServiceImpl<K, V> implements WithOffsetService<K, V> {

        final StartBuildingConsumerImpl<K, V> parentBuilder;

        WithOffsetServiceImpl(StartBuildingConsumerImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public WithLogger<K, V> errorStrategy(RecordErrorHandlingStrategy strategy) {
            this.parentBuilder.errorStrategy =
                    Objects.requireNonNull(strategy, "ErrorStrategy not set");
            return new WithLoggerImpl<>(parentBuilder);
        }
    }

    private static class WithLoggerImpl<K, V> implements WithLogger<K, V> {

        final StartBuildingConsumerImpl<K, V> parentBuilder;

        WithLoggerImpl(StartBuildingConsumerImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public WithOptionals<K, V> logger(Logger logger) {
            this.parentBuilder.logger = Objects.requireNonNull(logger, "Logger not set");
            return new WithOptionalsImpl<>(parentBuilder);
        }
    }

    private static class WithOptionalsImpl<K, V> implements WithOptionals<K, V> {
        final StartBuildingConsumerImpl<K, V> parentBuilder;

        WithOptionalsImpl(StartBuildingConsumerImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public WithOptionals<K, V> threads(int threads) {
            this.parentBuilder.threads = threads;
            return this;
        }

        @Override
        public WithOptionals<K, V> ordering(OrderStrategy orderStrategy) {
            this.parentBuilder.orderStrategy =
                    Objects.requireNonNull(orderStrategy, "OrderStrategy not set");
            return this;
        }

        @Override
        public WithOptionals<K, V> preferSingleThread(boolean singleThread) {
            this.parentBuilder.preferSingleThread = singleThread;
            return this;
        }

        @Override
        public WithOptionals<K, V> monitor(Monitor monitor) {
            this.parentBuilder.monitor = Objects.requireNonNull(monitor, "Monitor not set");
            return this;
        }

        @Override
        public RecordConsumer<K, V> build() {
            if (parentBuilder.threads < 1 && parentBuilder.threads != -1) {
                throw new IllegalArgumentException("Threads number must be greater than zero");
            }
            if (parentBuilder.threads != 1
                    && !parentBuilder.processor.processUpdatesType().allowConcurrentProcessing()) {
                throw new IllegalArgumentException(
                        "Command mode does not support parallel processing");
            }
            if (parentBuilder.threads == 1 && parentBuilder.preferSingleThread) {
                return new SingleThreadedRecordConsumer<>(parentBuilder);
            }
            return new ParallelRecordConsumer<>(parentBuilder);
        }
    }

    static interface ProcessUpdatesStrategy {

        default void processUpdates(
                MappedRecord record, Set<SubscribedItem> routable, EventListener listener) {
            final Map<String, String> updates = getEvent(record);
            sendUpdates(updates, routable, listener);
        }

        default void processUpdatesAsSnapshot(
                MappedRecord record, SubscribedItem subscribedItem, EventListener listener) {
            final Map<String, String> updates = getEvent(record);
            sendUpdatesAsSnapshot(updates, subscribedItem, listener);
        }

        default Map<String, String> getEvent(MappedRecord record) {
            return record.fieldsMap();
        }

        void sendUpdates(
                Map<String, String> updates, Set<SubscribedItem> routable, EventListener listener);

        default void sendUpdatesAsSnapshot(
                Map<String, String> updates,
                SubscribedItem subscribedItem,
                EventListener listener) {}

        void useLogger(Logger logger);

        Logger getLogger();

        ProcessUpdatesType type();

        static ProcessUpdatesStrategy fromCommandModeStrategy(
                CommandModeStrategy commandModeStrategy) {
            return switch (commandModeStrategy) {
                case NONE -> defaultStrategy();
                case ENFORCE -> commandStrategy();
                case AUTO -> autoCommandModeStrategy();
            };
        }

        static ProcessUpdatesStrategy defaultStrategy() {
            return new DefaultUpdatesStrategy();
        }

        static ProcessUpdatesStrategy commandStrategy() {
            return new CommandProcessUpdatesStrategy();
        }

        static ProcessUpdatesStrategy autoCommandModeStrategy() {
            return new AutoCommandModeProcessUpdatesStrategy();
        }
    }

    static class DefaultUpdatesStrategy implements ProcessUpdatesStrategy {

        private Logger logger = LoggerFactory.getLogger(ProcessUpdatesStrategy.class);

        @Override
        public void sendUpdates(
                Map<String, String> updates, Set<SubscribedItem> routable, EventListener listener) {
            for (SubscribedItem sub : routable) {
                sub.sendRealtimeEvent(updates, listener);
            }
        }

        @Override
        public void sendUpdatesAsSnapshot(
                Map<String, String> updates,
                SubscribedItem subscribedItem,
                EventListener listener) {
            subscribedItem.sendSnapshotEvent(updates, listener);
        }

        public final void useLogger(Logger logger) {
            this.logger = Objects.requireNonNullElse(logger, this.logger);
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

    static class AutoCommandModeProcessUpdatesStrategy extends DefaultUpdatesStrategy {

        @Override
        public Map<String, String> getEvent(MappedRecord record) {
            Map<String, String> event = record.fieldsMap();
            if (record.isPayloadNull()) {
                String key = Key.KEY.lookUp(event);
                getLogger()
                        .atDebug()
                        .log("Payload is null, sending DELETE command for key: %s", key);
                return CommandMode.deleteEvent(event);
            }

            return CommandMode.decorate(event, Command.ADD);
        }

        @Override
        public ProcessUpdatesType type() {
            return ProcessUpdatesType.AUTO_COMMAND_MODE;
        }
    }

    static interface CommandMode {

        static final String SNAPSHOT = "snapshot";

        static Map<String, String> decorate(Map<String, String> event, Command command) {
            event.put(Key.COMMAND.key(), command.toString());
            return event;
        }

        static Map<String, String> deleteEvent(Map<String, String> event) {
            // Creates a new event with only the key field: all other fields are discarded because
            // they are not relevant for the deletion operation.
            Map<String, String> deleteEvent = new HashMap<>();
            deleteEvent.put(Key.KEY.key(), Key.KEY.lookUp(event));

            // Decorate the event with DELETE command
            return decorate(deleteEvent, Command.DELETE);
        }

        enum Command {
            ADD,
            DELETE,
            UPDATE,
            CS,
            EOS;

            static Map<String, Command> CACHE =
                    Stream.of(values())
                            .collect(Collectors.toMap(Command::toString, Function.identity()));

            static Optional<Command> lookUp(Map<String, String> input) {
                String command = input.get(Key.COMMAND.key());
                return Optional.ofNullable(CACHE.get(command));
            }

            boolean isSnapshot() {
                return this.equals(CS) || this.equals(EOS);
            }
        }

        enum Key {
            KEY("key"),
            COMMAND("command");

            private final String key;

            Key(String key) {
                this.key = key;
            }

            String lookUp(Map<String, String> input) {
                return input.get(key);
            }

            String key() {
                return key;
            }
        }
    }

    static final class CommandProcessUpdatesStrategy extends DefaultUpdatesStrategy {

        CommandProcessUpdatesStrategy() {}

        @Override
        public void sendUpdates(
                Map<String, String> updates, Set<SubscribedItem> routable, EventListener listener) {
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
                    // updater.update(sub, updates, sub.isSnapshot());
                    if (sub.isSnapshot()) {
                        sub.sendSnapshotEvent(updates, listener);
                    } else {
                        sub.sendRealtimeEvent(updates, listener);
                    }
                }
            }
        }

        Optional<Command> checkInput(Map<String, String> input) {
            if (input == null) {
                return Optional.empty();
            }

            // Retrieve the value of the mandatory "key" field from the input map.
            String key = CommandMode.Key.KEY.lookUp(input);
            if (key == null || key.isBlank()) {
                return Optional.empty();
            }

            // Retrieve the value of the mandatory "command" field from the input map and
            Optional<Command> command = Command.lookUp(input);
            if (command.isEmpty()) {
                return command;
            }

            Command cmd = command.get();

            // If the key is "snapshot", we expect the command to be either CS or EOS.
            if (CommandMode.SNAPSHOT.equals(key)) {
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

        private void handleSnapshot(Command snapshot, SubscribedItem sub, EventListener listener) {
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

    static class DefaultRecordProcessor<K, V> implements RecordProcessor<K, V> {

        protected final RecordMapper<K, V> recordMapper;
        protected final ProcessUpdatesStrategy processUpdatesStrategy;
        protected final EventListener listener;
        protected final SubscribedItems subscribedItems;
        protected Logger logger = LoggerFactory.getLogger(DefaultRecordProcessor.class);

        DefaultRecordProcessor(
                RecordMapper<K, V> recordMapper,
                SubscribedItems subscribedItems,
                EventListener listener,
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
        public final void process(KafkaRecord<K, V> record) throws ValueException {
            MappedRecord mappedRecord = recordMapper.map(record);

            Set<SubscribedItem> routable = mappedRecord.route(subscribedItems);
            int size = routable.size();
            if (size > 0) {
                logger.atDebug().log("Routing record to {} items", size);
                processUpdatesStrategy.processUpdates(mappedRecord, routable, listener);
            } else {
                logger.atDebug().log("No routable items found");
            }
        }

        @Override
        public void processAsSnapshot(KafkaRecord<K, V> record, SubscribedItem subscribedItem)
                throws ValueException {
            logger.atDebug().log("Mapping incoming Kafka record for subscribed item");
            MappedRecord mappedRecord = recordMapper.map(record);
            processUpdatesStrategy.processUpdatesAsSnapshot(mappedRecord, subscribedItem, listener);
        }

        @Override
        public ProcessUpdatesType processUpdatesType() {
            return processUpdatesStrategy.type();
        }
    }

    abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected final OffsetService offsetService;
        protected final RecordProcessor<K, V> recordProcessor;
        protected final Logger logger;
        protected final RecordErrorHandlingStrategy errorStrategy;
        protected volatile Throwable firstFailure = null;

        private volatile boolean closed = false;

        protected final Monitor monitor;
        protected final Meters.Counter receivedRecordCounter;
        protected final Meters.Counter processedRecordCounter;
        protected final RecordBatchListener recordBatchListener;

        AbstractRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            this.errorStrategy = builder.errorStrategy;
            this.offsetService = builder.offsetService;
            this.logger = builder.logger;
            this.recordProcessor = builder.processor;
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
                monitor.observe(receivedRecordCounter).enableIrate().enableRate();
                monitor.observe(processedRecordCounter).enableIrate().enableRate();
            }
        }

        @Override
        public final void consumeBatch(RecordBatch<K, V> batch) {
            receivedRecordCounter.increment(batch.count());
            consumeRecordBatch(batch);
            offsetService.maybeCommit();
        }

        @Override
        public void close() {
            this.closed = true;
            onPoolsShutdown();
        }

        @Override
        public boolean hasFailedAsynchronously() {
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
        public final Monitor monitor() {
            return monitor;
        }

        abstract void consumeRecordBatch(RecordBatch<K, V> batch);

        /** Hook for subclasses to shutdown additional pools before offset commit. */
        void onPoolsShutdown() {
            // Default: nothing, subclasses can override
        }

        final void asyncFailure(Throwable t) {
            if (firstFailure == null) {
                firstFailure = t;
            }
        }

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

    static class SingleThreadedRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        SingleThreadedRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
        }

        @Override
        void consumeRecordBatch(RecordBatch<K, V> batch) {
            for (KafkaRecord<K, V> record : batch.getRecords()) {
                consumeRecord(record);
            }
        }

        private void consumeRecord(KafkaRecord<K, V> record) {
            try {
                recordProcessor.process(record);
                offsetService.updateOffsets(record);
            } catch (ValueException ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy());

                switch (errorStrategy()) {
                    case IGNORE_AND_CONTINUE -> {
                        // Log the error here to catch the stack trace
                        logger.atWarn().setCause(ve).log("Ignoring error");
                        offsetService.updateOffsets(record);
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
    }

    static class ParallelRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        private static final String POOL_NAME = "Ring buffer processor pool";
        private static final int RING_BUFFER_CAPACITY = 16_384;

        protected final OrderStrategy orderStrategy;
        protected final int configuredThreads;
        protected final int actualThreads;

        private final BlockingQueue<KafkaRecord<K, V>>[] ringBuffers;
        private final AtomicLong roundRobinCounter = new AtomicLong(0);
        private final ExecutorService ringBufferPool;
        private volatile boolean stopping = false;

        @SuppressWarnings("unchecked")
        ParallelRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
            this.orderStrategy = builder.orderStrategy;
            this.configuredThreads = builder.threads;
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

            // Initialize ring buffers and submit processing tasks
            for (int i = 0; i < actualThreads; i++) {
                // Use calculated optimal capacity for maximum throughput
                this.ringBuffers[i] = new ArrayBlockingQueue<>(RING_BUFFER_CAPACITY);
                final int threadIndex = i;
                configureMonitor(threadIndex);

                logger.atDebug().log(
                        "Initialized ring buffer {} with capacity {}", i, RING_BUFFER_CAPACITY);

                // Submit ring buffer processing task to executor
                ringBufferPool.submit(() -> processRingBuffer(threadIndex));
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
                    .enableAverage();
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
        public Optional<OrderStrategy> orderStrategy() {
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
                    feedBuffer(record, bufferIndex);
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

        // Private methods
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

        private void feedBuffer(KafkaRecord<K, V> record, int bufferIndex)
                throws InterruptedException {
            ringBuffers[bufferIndex].put(record);
        }

        private void consume(KafkaRecord<K, V> record) {
            try {
                recordProcessor.process(record);
                offsetService.updateOffsets(record);
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

        private void processRingBuffer(int threadIndex) {
            final BlockingQueue<KafkaRecord<K, V>> ringBuffer = ringBuffers[threadIndex];

            if (logger.isDebugEnabled()) {
                logger.debug("Starting processing thread {}", threadIndex);
            }

            // Continue processing until stopping
            while (!stopping) {
                try {
                    // Efficient batch drain - reuse ArrayList to minimize GC
                    KafkaRecord<K, V> record = ringBuffer.poll(10, TimeUnit.MILLISECONDS);
                    if (record == null) continue;
                    consume(record);
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
                try {
                    consume(record);
                } catch (Exception e) {
                    logger.atError()
                            .setCause(e)
                            .log(
                                    "Ingornig error while draining remaining records in ring buffer processor {} after shutdown",
                                    threadIndex);
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Stopped processing thread {}", threadIndex);
            }
        }

        private void handleError(KafkaRecord<K, V> record, ValueException ve) {
            switch (errorStrategy()) {
                case IGNORE_AND_CONTINUE -> {
                    logger.atWarn().log("Ignoring error");
                    offsetService.updateOffsets(record);
                }

                case FORCE_UNSUBSCRIPTION -> {
                    logger.atWarn().log("Will force unsubscription");
                    asyncFailure(ve);
                }
            }
        }
    }

    private RecordConsumerSupport() {}
}
