
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordsBatch;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.StartBuildingConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.StartBuildingProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithEnforceCommandMode;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithLogger;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOptionals;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

class RecordConsumerSupport {

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
        protected ItemEventListener listener;
        protected boolean enforceCommandMode;

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
        public WithEnforceCommandMode<K, V> enforceCommandMode(boolean enforceCommandMode) {
            this.parentBuilder.enforceCommandMode = enforceCommandMode;
            return new WithEnforceCommandModeImpl<>(parentBuilder);
        }
    }

    private static class WithEnforceCommandModeImpl<K, V> implements WithEnforceCommandMode<K, V> {
        final StartBuildingProcessorBuilderImpl<K, V> parentBuilder;

        WithEnforceCommandModeImpl(StartBuildingProcessorBuilderImpl<K, V> parentBuilder) {
            this.parentBuilder = parentBuilder;
        }

        @Override
        public StartBuildingConsumer<K, V> eventListener(ItemEventListener listener) {
            this.parentBuilder.listener =
                    Objects.requireNonNull(listener, "ItemEventListener not set");

            RecordProcessor<K, V> recordProcessor =
                    this.parentBuilder.enforceCommandMode
                            ? new CommandRecordProcessor<>(
                                    this.parentBuilder.mapper,
                                    this.parentBuilder.subscribed,
                                    this.parentBuilder.listener)
                            : new DefaultRecordProcessor<>(
                                    this.parentBuilder.mapper,
                                    this.parentBuilder.subscribed,
                                    this.parentBuilder.listener);
            return new StartBuildingConsumerImpl<>(recordProcessor);
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
        public RecordConsumer<K, V> build() {
            if (parentBuilder.threads < 1 && parentBuilder.threads != -1) {
                throw new IllegalArgumentException("Threads number must be greater than zero");
            }
            if (parentBuilder.threads > 1 && parentBuilder.processor.isCommandEnforceEnabled()) {
                throw new IllegalArgumentException(
                        "Command mode does not support parallel processing");
            }
            if (parentBuilder.threads == 1 && parentBuilder.preferSingleThread) {
                return new SingleThreadedRecordConsumer<>(parentBuilder);
            }
            return new ParallelRecordConsumer<>(parentBuilder);
        }
    }

    static sealed class DefaultRecordProcessor<K, V> implements RecordProcessor<K, V>
            permits CommandRecordProcessor {

        protected final RecordMapper<K, V> recordMapper;
        protected final SubscribedItems subscribedItems;
        protected final ItemEventListener listener;
        protected Logger logger = LoggerFactory.getLogger(DefaultRecordProcessor.class);

        DefaultRecordProcessor(
                RecordMapper<K, V> recordMapper,
                SubscribedItems subscribedItems,
                ItemEventListener listener) {
            this.recordMapper = recordMapper;
            this.subscribedItems = subscribedItems;
            this.listener = listener;
        }

        @Override
        public final void useLogger(Logger logger) {
            this.logger = Objects.requireNonNullElse(logger, this.logger);
        }

        @Override
        public final void process(ConsumerRecord<K, V> record) throws ValueException {
            logger.atTrace()
                    .log(() -> "Mapping incoming Kafka record: %s".formatted(record.toString()));

            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));

            // As logging the mapped record is expensive, log lazily it only at trace level.
            logger.atTrace().log("Mapped Kafka record");

            Set<SubscribedItem> routable = mappedRecord.route(subscribedItems);
            if (routable.size() > 0) {
                logger.atTrace().log("Routing record to {} items", routable.size());
                processUpdates(mappedRecord.fieldsMap(), routable);
            } else {
                logger.atTrace().log("No routable items found");
            }
        }

        protected void processUpdates(Map<String, String> updates, Set<SubscribedItem> routable) {
            for (SubscribedItem sub : routable) {
                logger.atTrace().log(() -> "Sending updates: %s".formatted(updates));
                listener.smartUpdate(sub.itemHandle(), updates, false);
            }
        }
    }

    static final class CommandRecordProcessor<K, V> extends DefaultRecordProcessor<K, V> {

        private enum Command {
            ADD,
            DELETE,
            UPDATE;
        }

        private enum Snapshot {
            CS,
            EOS;
        }

        private enum CommandKey {
            KEY("key"),
            COMMAND("command");

            private final String key;

            CommandKey(String key) {
                this.key = key;
            }

            String lookUp(Map<String, String> input) {
                return input.get(key);
            }

            boolean contains(Map<String, String> input) {
                return input.containsKey(key);
            }
        }

        private static String SNAPSHOT = "snapshot";

        CommandRecordProcessor(
                RecordMapper<K, V> recordMapper,
                SubscribedItems subscribedItems,
                ItemEventListener listener) {
            super(recordMapper, subscribedItems, listener);
        }

        @Override
        protected void processUpdates(Map<String, String> updates, Set<SubscribedItem> routable) {
            if (!checkInput(updates)) {
                logger.atWarn()
                        .log(
                                "Discarding record due to command mode fields not properly valued: key {} - command {}",
                                CommandKey.KEY.lookUp(updates),
                                CommandKey.COMMAND.lookUp(updates));
                return;
            }

            String snapshotOrCommand = CommandKey.COMMAND.lookUp(updates);
            for (SubscribedItem sub : routable) {
                logger.atTrace().log(() -> "Sending updates: %s".formatted(updates));
                logger.atTrace().log("Enforce COMMAND mode semantic of records read");

                if (SNAPSHOT.equals(CommandKey.KEY.lookUp(updates))) {
                    handleSnapshot(Snapshot.valueOf(snapshotOrCommand), sub);
                } else {
                    handleCommand(Command.valueOf(snapshotOrCommand), updates, sub);
                }
            }
        }

        @Override
        public boolean isCommandEnforceEnabled() {
            return true;
        }

        boolean checkInput(Map<String, String> input) {
            if (input == null) {
                return false;
            }

            String key = CommandKey.KEY.lookUp(input);
            if (key == null || key.isBlank()) {
                return false;
            }

            if (!CommandKey.COMMAND.contains(input)) {
                return false;
            }

            String snapshotOrCommand = CommandKey.COMMAND.lookUp(input);
            if (snapshotOrCommand == null) {
                return false;
            }

            if (SNAPSHOT.equals(key)) {
                return switch (snapshotOrCommand) {
                    case "CS", "EOS" -> true;
                    default -> false;
                };
            }
            return switch (snapshotOrCommand) {
                case "ADD", "DELETE", "UPDATE" -> true;
                default -> false;
            };
        }

        private void handleSnapshot(Snapshot snapshotCommand, SubscribedItem sub) {
            switch (snapshotCommand) {
                case CS -> {
                    logger.atTrace().log("Sending clearSnapshot");
                    listener.smartClearSnapshot(sub.itemHandle());
                    sub.setSnapshot(true);
                }
                case EOS -> {
                    logger.atTrace().log("Sending endOfSnapshot");
                    listener.smartEndOfSnapshot(sub.itemHandle());
                    sub.setSnapshot(false);
                }
            }
        }

        void handleCommand(Command command, Map<String, String> updates, SubscribedItem sub) {
            logger.atTrace().log("Sending {} command", command);
            listener.smartUpdate(sub.itemHandle(), updates, sub.isSnapshot());
        }
    }

    private abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected final OffsetService offsetService;
        protected final RecordProcessor<K, V> recordProcessor;
        protected final Logger logger;
        private final RecordErrorHandlingStrategy errorStrategy;

        AbstractRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            this.errorStrategy = builder.errorStrategy;
            this.offsetService = builder.offsetService;
            this.logger = builder.logger;
            this.recordProcessor = builder.processor;
            // Enforce usage of the same logger
            this.recordProcessor.useLogger(logger);
        }

        @Override
        public void close() {
            offsetService.onConsumerShutdown();
        }

        @Override
        public boolean isCommandEnforceEnabled() {
            return recordProcessor.isCommandEnforceEnabled();
        }

        @Override
        public RecordErrorHandlingStrategy errorStrategy() {
            return errorStrategy;
        }
    }

    /**
     * Centralized performance monitoring with sliding window measurement. Provides accurate
     * throughput statistics without synchronization overhead.
     */
    private static class PerformanceMonitor {

        private static final String LOGGER_SUFFIX = "Performance";

        private final RecordsCounter recordsCounter;
        private final String monitorName;
        private final Logger logger;
        private final AtomicLong lastCheck = new AtomicLong(System.currentTimeMillis());
        private final AtomicLong lastReportedCount = new AtomicLong(0);

        // Ring buffer utilization monitoring
        private volatile long lastUtilizationCheck = System.currentTimeMillis();
        private static final long UTILIZATION_CHECK_INTERVAL_MS = 2000; // Check every 2 seconds
        private volatile int[] peakUtilizationSinceLastReport; // Track peak utilization per buffer

        PerformanceMonitor(String monitorName, RecordsCounter recordsCounter, Logger logger) {
            this.recordsCounter = recordsCounter;
            this.monitorName = monitorName;
            this.logger = LoggerFactory.getLogger(logger.getName() + LOGGER_SUFFIX);
        }

        /** Check and potentially log performance stats every 5 seconds using sliding window */
        void checkStats() {
            long currentTime = System.currentTimeMillis();
            long lastCheckTime = lastCheck.get();

            // Report stats every 5 seconds
            if (currentTime - lastCheckTime > 5000
                    && lastCheck.compareAndSet(lastCheckTime, currentTime)) {
                long currentTotal = recordsCounter.getTotalRecords();
                long lastTotal = lastReportedCount.getAndSet(currentTotal);

                long recordsInWindow = currentTotal - lastTotal;
                long timeWindowMs = currentTime - lastCheckTime;

                if (recordsInWindow > 0 && timeWindowMs > 0) {
                    long avgThroughput = (recordsInWindow * 1000L) / timeWindowMs;
                    logger.atInfo().log(
                            "{} processing stats: {} records processed in {}ms window, avg {}k records/sec",
                            monitorName,
                            recordsInWindow,
                            timeWindowMs,
                            avgThroughput / 1000.0);
                }
            }
        }

        /**
         * Monitor and graphically display ring buffer utilization rates. Provides visual
         * representation of buffer load across all threads.
         */
        void checkRingBufferUtilization(BlockingQueue<?>[] ringBuffers, int ringBufferCapacity) {
            if (ringBuffers == null || ringBuffers.length == 0) {
                return;
            }

            // Initialize peak tracking if not already done
            if (peakUtilizationSinceLastReport == null) {
                peakUtilizationSinceLastReport = new int[ringBuffers.length];
            }

            // Update peak utilization continuously
            for (int i = 0; i < ringBuffers.length; i++) {
                int currentUtilization = (ringBuffers[i].size() * 100) / ringBufferCapacity;
                peakUtilizationSinceLastReport[i] =
                        Math.max(peakUtilizationSinceLastReport[i], currentUtilization);
            }

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastUtilizationCheck < UTILIZATION_CHECK_INTERVAL_MS) {
                return;
            }
            lastUtilizationCheck = currentTime;

            int actualThreads = ringBuffers.length;
            StringBuilder utilizationReport = new StringBuilder();
            utilizationReport.append("\n┌─ Ring Buffer Utilization Report (2s interval) ─┐\n");

            int totalUsed = 0;
            int totalCapacity = ringBufferCapacity * actualThreads;
            int totalPeakUsed = 0;

            for (int i = 0; i < actualThreads; i++) {
                int currentSize = ringBuffers[i].size();
                int utilization = (currentSize * 100) / ringBufferCapacity;
                int peakUtilization = peakUtilizationSinceLastReport[i];
                totalUsed += currentSize;
                totalPeakUsed += (peakUtilization * ringBufferCapacity) / 100;

                // Create visual bars (20 chars wide)
                String currentBar = createUtilizationBar(utilization, 20);
                String peakBar = createUtilizationBar(peakUtilization, 20);

                utilizationReport.append(
                        String.format(
                                "│ Buf[%2d]: Now[%s] %3d%% Peak[%s] %3d%% (%d/%d)\n",
                                i,
                                currentBar,
                                utilization,
                                peakBar,
                                peakUtilization,
                                currentSize,
                                ringBufferCapacity));
            }

            // Overall utilization
            int overallUtilization = (totalUsed * 100) / totalCapacity;
            int overallPeakUtilization = (totalPeakUsed * 100) / totalCapacity;
            String overallBar = createUtilizationBar(overallUtilization, 25);
            String overallPeakBar = createUtilizationBar(overallPeakUtilization, 25);

            utilizationReport.append("├─────────────────────────────────────────────────┤\n");
            utilizationReport.append(
                    String.format(
                            "│ Overall Now: [%s] %3d%% (%d/%d)\n",
                            overallBar, overallUtilization, totalUsed, totalCapacity));
            utilizationReport.append(
                    String.format(
                            "│ Overall Peak:[%s] %3d%% (%d/%d)\n",
                            overallPeakBar, overallPeakUtilization, totalPeakUsed, totalCapacity));
            utilizationReport.append("└─────────────────────────────────────────────────┘");

            // Log based on peak utilization (more meaningful than current)
            if (overallPeakUtilization > 80) {
                logger.atInfo().log(
                        "HIGH peak ring buffer utilization detected:{}", utilizationReport);
            } else if (overallPeakUtilization > 40 || overallUtilization > 20) {
                logger.atInfo().log("Ring buffer utilization status:{}", utilizationReport);
            } else {
                logger.atInfo().log(
                        "Ring buffer utilization status (low activity):{}", utilizationReport);
            }

            // Reset peak tracking for next interval
            Arrays.fill(peakUtilizationSinceLastReport, 0);
        }

        /**
         * Create a visual utilization bar with color indicators.
         *
         * @param utilizationPercent The utilization percentage (0-100)
         * @param width The width of the bar in characters
         * @return A string representing the visual bar
         */
        private String createUtilizationBar(int utilizationPercent, int width) {
            int filledChars = (utilizationPercent * width) / 100;
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
    }

    static class SingleThreadedRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        private static final RecordsBatch NO_OP = () -> {};

        // Performance monitoring for single-threaded scenarios
        private final PerformanceMonitor performanceMonitor;
        private final RecordsCounter recordsCounter;

        SingleThreadedRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
            this.recordsCounter = new RecordsCounter();
            this.performanceMonitor =
                    new PerformanceMonitor("Single-thread", this.recordsCounter, logger);
        }

        @Override
        public RecordsBatch consumeRecords(ConsumerRecords<K, V> records) {
            int recordCount = records.count();
            if (recordCount > 0) {
                for (ConsumerRecord<K, V> record : records) {
                    consumeRecord(record);
                }
                recordsCounter.count(recordCount);
            }

            offsetService.maybeCommit();

            // Check processing stats periodically
            performanceMonitor.checkStats();

            return NO_OP;
        }

        void consumeRecord(ConsumerRecord<K, V> record) {
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
                logger.atError().log("Serious error while processing record!");
                throw new KafkaException(t);
            }
        }
    }

    static class RecordsBatchImpl<K, V> implements RecordsBatch {

        private final CountDownLatch latch;
        private final ConsumerRecords<K, V> records;
        private final int recordsCount;
        private final AtomicInteger processedCount = new AtomicInteger(0);
        private final Logger logger;
        private final long batchId;
        private final RecordsCounter recordsCounter;

        RecordsBatchImpl(
                ConsumerRecords<K, V> records,
                Logger logger,
                long batchId,
                RecordsCounter recordsCounter) {
            this.recordsCount = records.count();
            this.records = records;
            this.latch = new CountDownLatch(recordsCount);
            this.logger = logger;
            this.batchId = batchId;
            this.recordsCounter = recordsCounter; // Use the passed parameter
            this.logger.atDebug().log("Created batch {} with {} records", batchId, recordsCount);
        }

        ConsumerRecords<K, V> getRecords() {
            return records;
        }

        @Override
        public int count() {
            return recordsCount;
        }

        void recordProcessed() {
            latch.countDown();
            int done = processedCount.incrementAndGet();
            if (done == recordsCount) {
                recordsCounter.count(recordsCount);
                logger.debug(
                        "Batch {} processed all {} records - ready for commit",
                        batchId,
                        recordsCount);
                return;
            }

            if (done > recordsCount) {
                logger.warn(
                        "More records processed ({}) than expected ({}) in batch {}",
                        done,
                        recordsCount,
                        batchId);
            }
        }

        @Override
        public void join() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted while waiting for records batch to complete", e);
            }
        }
    }

    static class RecordsCounter {

        private final AtomicLong totalRecordsCounter = new AtomicLong(0);

        void count(int count) {
            totalRecordsCounter.addAndGet(count);
        }

        long getTotalRecords() {
            return totalRecordsCounter.get();
        }
    }

    static class RecordWithBatch<K, V> {
        final ConsumerRecord<K, V> record;
        final RecordsBatchImpl<K, V> batch;

        RecordWithBatch(ConsumerRecord<K, V> record, RecordsBatchImpl<K, V> batch) {
            this.record = record;
            this.batch = batch;
        }

        // Factory method to reduce allocation overhead
        static <K, V> RecordWithBatch<K, V> of(
                ConsumerRecord<K, V> record, RecordsBatchImpl<K, V> batch) {
            return new RecordWithBatch<>(record, batch);
        }
    }

    static class ParallelRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        protected final OrderStrategy orderStrategy;
        protected final int configuredThreads;
        protected final int actualThreads;

        // Ring buffers settings for high-throughput processing
        private final BlockingQueue<RecordWithBatch<K, V>>[] ringBuffers;
        private final AtomicLong roundRobinCounter = new AtomicLong(0);
        private final ExecutorService ringBufferPool;
        private volatile boolean shutdownRequested = false;
        private final int ringBufferCapacity;

        // Performance monitoring without synchronization overhead
        private final PerformanceMonitor performanceMonitor;
        private final RecordsCounter recordsCounter = new RecordsCounter();

        private final AtomicLong batchIdCounter = new AtomicLong(0);

        @SuppressWarnings("unchecked")
        ParallelRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
            this.orderStrategy = builder.orderStrategy;
            this.configuredThreads = builder.threads;
            this.actualThreads = getActualThreadsNumber(builder.threads);

            // Calculate optimal ring buffer capacity based on workload
            this.ringBufferCapacity = calculateOptimalRingBufferCapacity();

            // Initialize performance monitor
            this.performanceMonitor = new PerformanceMonitor("Parallel", recordsCounter, logger);

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
                ringBuffers[i] = new ArrayBlockingQueue<>(ringBufferCapacity);
                final int threadIndex = i;

                logger.atDebug().log(
                        "Initialized ring buffer {} with capacity {}", i, ringBufferCapacity);

                // Submit ring buffer processing task to executor
                ringBufferPool.submit(() -> processRingBuffer(threadIndex));
            }
        }

        /**
         * Calculate optimal ring buffer capacity based on throughput requirements and thread
         * configuration. Uses workload analysis rather than arbitrary values.
         */
        private int calculateOptimalRingBufferCapacity() {
            // Target throughput analysis - aiming for 1M+ records/sec
            int targetTotalThroughputPerSec = 1_000_000; // Target 1M records/sec
            int targetThroughputPerThread = targetTotalThroughputPerSec / actualThreads;

            // Processing time estimation (optimistic for high throughput)
            int estimatedProcessingTimeMs = 1; // Assume 1ms avg processing per record
            int batchDrainSize = 5000; // From processRingBuffer batch size

            // Calculate buffer needs
            int recordsPerMs = Math.max(1, targetThroughputPerThread / 1000);
            int bufferForProcessingPipeline = recordsPerMs * estimatedProcessingTimeMs;
            int bufferForBatchingEfficiency = batchDrainSize * 3; // Triple buffer for efficiency

            // Safety margin for burst traffic (50% overhead for high throughput)
            int burstCapacity = Math.max(2000, bufferForProcessingPipeline / 2);

            // Total capacity calculation
            int calculatedCapacity =
                    bufferForProcessingPipeline + bufferForBatchingEfficiency + burstCapacity;

            // Apply reasonable bounds - increase for high throughput
            int minCapacity = 8000; // Higher minimum for high throughput
            int maxCapacity = 131072; // Increased maximum capacity

            int finalCapacity = Math.max(minCapacity, Math.min(calculatedCapacity, maxCapacity));

            logger.atInfo().log(
                    "Ring buffer capacity calculation: {} threads, {}k target throughput/thread, {}ms processing time, {} final capacity",
                    actualThreads,
                    targetThroughputPerThread / 1000,
                    estimatedProcessingTimeMs,
                    finalCapacity);

            return finalCapacity;
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
        public RecordsBatch consumeRecords(ConsumerRecords<K, V> records) {
            if (records.isEmpty()) {
                offsetService.maybeCommit();
                return RecordsBatch.nop();
            }

            long startTime = System.nanoTime();
            int recordCount = records.count();
            RecordsBatchImpl<K, V> batch =
                    new RecordsBatchImpl<>(
                            records, logger, batchIdCounter.getAndIncrement(), recordsCounter);

            processWithRingBuffers(batch);
            offsetService.maybeCommit();

            long totalTime = (System.nanoTime() - startTime) / 1_000_000;
            if (totalTime > 10) { // Only log if processing takes >10ms
                long batchThroughput = (recordCount * 1000L) / Math.max(1, totalTime);
                logger.atDebug().log(
                        "Batch: {} records in {}ms ({}k records/sec)",
                        recordCount,
                        totalTime,
                        batchThroughput / 1000.0);
            }
            // Check processing stats periodically
            performanceMonitor.checkStats();

            // Check and log ring buffer utilization periodically
            performanceMonitor.checkRingBufferUtilization(ringBuffers, ringBufferCapacity);

            Throwable failure = offsetService.getFirstFailure();
            if (failure != null) {
                logger.atWarn().log("Forcing unsubscription");
                throw new KafkaException(failure);
            }
            return batch;
        }

        // Ring buffer processing for high-throughput scenarios
        private void processWithRingBuffers(RecordsBatchImpl<K, V> batch) {
            // Distribute records to ring buffers
            for (ConsumerRecord<K, V> record : batch.getRecords()) {
                int bufferIndex = selectRingBuffer(record);
                try {
                    // Use blocking put to apply backpressure instead of failing
                    ringBuffers[bufferIndex].put(RecordWithBatch.of(record, batch));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.atWarn()
                            .log(
                                    "Interrupted while distributing records to ring buffers, stopping batch distribution early");
                    return; // Exit gracefully, allow partial batch to complete
                }
            }
        }

        private int selectRingBuffer(ConsumerRecord<K, V> record) {
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

        private void processRingBuffer(int threadIndex) {
            final BlockingQueue<RecordWithBatch<K, V>> ringBuffer = ringBuffers[threadIndex];
            // Pre-allocate with optimal size and reuse to minimize GC pressure
            final List<RecordWithBatch<K, V>> batchBuffer = new FixedList<>(5000);

            if (logger.isDebugEnabled()) {
                logger.debug("Starting processing thread {}", threadIndex);
            }

            // Continue processing until shutdown is requested AND the buffer is fully drained
            // This ensures all records are processed before exit on shutdown
            while (!shutdownRequested || !ringBuffer.isEmpty()) {
                try {
                    // Efficient batch drain - reuse ArrayList to minimize GC
                    RecordWithBatch<K, V> firstRecord = ringBuffer.poll(10, TimeUnit.MILLISECONDS);
                    if (firstRecord == null) continue;

                    batchBuffer.clear(); // Reset size but keep capacity
                    batchBuffer.add(firstRecord);
                    int additionalRecords =
                            ringBuffer.drainTo(batchBuffer, 4999); // Drain up to 4999 more

                    final int batchSize =
                            1 + additionalRecords; // Calculate directly: firstRecord + drained
                    for (int i = 0; i < batchSize; i++) {
                        consume(batchBuffer.get(i)); // Direct index access is faster
                    }

                    // Update records processed counter
                    recordsCounter.count(batchSize);
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

            if (logger.isDebugEnabled()) {
                logger.debug("Stopped processing thread {}", threadIndex);
            }
        }

        void consume(RecordWithBatch<K, V> record) {
            ConsumerRecord<K, V> rec = record.record;
            RecordsBatchImpl<K, V> batch = record.batch;
            try {
                recordProcessor.process(rec);
                offsetService.updateOffsets(rec);
            } catch (ValueException ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy());
                handleError(rec, ve);
            } catch (Throwable t) {
                logger.atError().log("Serious error while processing record!");
                offsetService.onAsyncFailure(t);
            } finally {
                batch.recordProcessed();
            }
        }

        private void handleError(ConsumerRecord<K, V> record, ValueException ve) {
            switch (errorStrategy()) {
                case IGNORE_AND_CONTINUE -> {
                    logger.atWarn().log("Ignoring error");
                    offsetService.updateOffsets(record);
                }

                case FORCE_UNSUBSCRIPTION -> {
                    // Trying to emulate the behavior of the above synchronous case, whereas the
                    // first record which gets an error ends the commits; hence we will keep track
                    // of the first error found on each partition; then, when committing each
                    // partition after the termination of the current poll, we will end before
                    // the first failed record found; this, obviously, is not possible in the
                    // fire-and-forget policy, but requires the batching one.
                    //
                    // There is a difference, though: in the synchronous case, the first error also
                    // stops the elaboration on the whole topic and all partitions are committed to
                    // the current point; in this case, instead, the elaboration continues until
                    // the end of the poll, hence, partitions with errors are committed up to
                    // the first error, but subsequent records, though not committed, may have
                    // been processed all the same (even records with the same key of a previously
                    // failed  record) and only then is the elaboration stopped.
                    // On the other hand, partitions without errors are processed and committed
                    // entirely, which is good, although, then, the elaboration stops also for them.
                    logger.atWarn().log("Will force unsubscription");
                    offsetService.onAsyncFailure(ve);
                }
            }
        }

        @Override
        public void close() {
            logger.atInfo().log("Shutting down high-throughput ring buffer processors");
            shutdownRequested = true;

            // Graceful shutdown of ring buffer processor pool
            ringBufferPool.shutdown();
            try {
                if (!ringBufferPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.atWarn()
                            .log(
                                    "Ring buffer processors did not terminate gracefully, forcing shutdown");
                    ringBufferPool.shutdownNow();
                    if (!ringBufferPool.awaitTermination(2, TimeUnit.SECONDS)) {
                        logger.atWarn()
                                .log(
                                        "Ring buffer processors did not terminate after forced shutdown");
                    }
                }
            } catch (InterruptedException e) {
                logger.atWarn()
                        .log("Interrupted while waiting for ring buffer processors to terminate");
                ringBufferPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            super.close();
        }

        public static <K, V> List<ConsumerRecord<K, V>> flatRecords(ConsumerRecords<K, V> records) {
            List<ConsumerRecord<K, V>> allRecords = new ArrayList<>(records.count());
            records.forEach(allRecords::add);
            return allRecords;
        }
    }

    private RecordConsumerSupport() {}
}
