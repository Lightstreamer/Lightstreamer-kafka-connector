
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

import static java.util.concurrent.Executors.newFixedThreadPool;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
        protected Logger log = LoggerFactory.getLogger(DefaultRecordProcessor.class);

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
            this.log = Objects.requireNonNullElse(logger, this.log);
        }

        @Override
        public final void process(ConsumerRecord<K, V> record) throws ValueException {
            log.atDebug().log(() -> "Mapping incoming Kafka record");
            log.atTrace().log(() -> "Kafka record: %s".formatted(record.toString()));

            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));

            // As logging the mapped record is expensive, log lazily it only at trace level.
            log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
            log.atDebug().log(() -> "Mapped Kafka record");

            Set<SubscribedItem> routable = mappedRecord.route(subscribedItems);
            if (routable.size() > 0) {
                log.atDebug().log(() -> "Filtering updates");
                Map<String, String> updates = mappedRecord.fieldsMap();

                log.atDebug().log("Routing record to {} items", routable.size());
                processUpdates(updates, routable);
            } else {
                log.atDebug().log("No routable items found");
            }
        }

        protected void processUpdates(Map<String, String> updates, Set<SubscribedItem> routable) {
            for (SubscribedItem sub : routable) {
                log.atDebug().log(() -> "Sending updates: %s".formatted(updates));
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
                log.atWarn()
                        .log(
                                "Discarding record due to command mode fields not properly valued: key {} - command {}",
                                CommandKey.KEY.lookUp(updates),
                                CommandKey.COMMAND.lookUp(updates));
                return;
            }

            String snapshotOrCommand = CommandKey.COMMAND.lookUp(updates);
            for (SubscribedItem sub : routable) {
                log.atDebug().log(() -> "Sending updates: %s".formatted(updates));
                log.atDebug().log("Enforce COMMAND mode semantic of records read");

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
                    log.atDebug().log("Sending clearSnapshot");
                    listener.smartClearSnapshot(sub.itemHandle());
                    sub.setSnapshot(true);
                }
                case EOS -> {
                    log.atDebug().log("Sending endOfSnapshot");
                    listener.smartEndOfSnapshot(sub.itemHandle());
                    sub.setSnapshot(false);
                }
            }
        }

        void handleCommand(Command command, Map<String, String> updates, SubscribedItem sub) {
            log.atDebug().log("Sending {} command", command);
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
            offsetService.commitSyncAndIgnoreErrors();
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

    static class SingleThreadedRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        SingleThreadedRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
        }

        @Override
        public void consumeRecords(ConsumerRecords<K, V> records) {
            records.forEach(this::consumeRecord);
            offsetService.commitAsync();
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

    static class ParallelRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        protected final OrderStrategy orderStrategy;
        protected final int configuredThreads;
        protected final TaskExecutor<String> taskExecutor;
        protected final int actualThreads;

        ParallelRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            super(builder);
            this.orderStrategy = builder.orderStrategy;
            this.configuredThreads = builder.threads;
            this.actualThreads = getActualThreadsNumber(configuredThreads);

            AtomicInteger threadCount = new AtomicInteger();
            this.taskExecutor =
                    TaskExecutor.create(
                            newFixedThreadPool(actualThreads, r -> newThread(r, threadCount)));
        }

        private static int getActualThreadsNumber(int configuredThreads) {
            if (configuredThreads == -1) {
                return Runtime.getRuntime().availableProcessors();
            }
            return configuredThreads;
        }

        private Thread newThread(Runnable r, AtomicInteger threadCount) {
            return new Thread(r, "ParallelConsumer-" + threadCount.getAndIncrement());
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
        public void consumeRecords(ConsumerRecords<K, V> records) {
            List<ConsumerRecord<K, V>> allRecords = flatRecords(records);
            taskExecutor.executeBatch(allRecords, orderStrategy::getSequence, this::consume);
            // NOTE: this may be inefficient if, for instance, a single task
            // should keep the executor engaged while all other threads are idle,
            // but this is not expected when all tasks are short and cpu-bound
            offsetService.commitAsync();
            Throwable failure = offsetService.getFirstFailure();
            if (failure != null) {
                logger.atWarn().log("Forcing unsubscription");
                throw new KafkaException(failure);
            }
        }

        void consume(String sequence, ConsumerRecord<K, V> record) {
            try {
                logger.atDebug().log(
                        () ->
                                "Processing record with sequence %s [%s]"
                                        .formatted(sequence, orderStrategy));
                recordProcessor.process(record);
                offsetService.updateOffsets(record);
            } catch (ValueException ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy());
                handleError(record, ve);
            } catch (Throwable t) {
                logger.atError().log("Serious error while processing record!");
                offsetService.onAsyncFailure(t);
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
            super.close();
            taskExecutor.shutdown();
        }
    }

    public static <K, V> List<ConsumerRecord<K, V>> flatRecords(ConsumerRecords<K, V> records) {
        List<ConsumerRecord<K, V>> allRecords = new ArrayList<>(records.count());
        records.partitions()
                .forEach(topicPartition -> allRecords.addAll(records.records(topicPartition)));
        return allRecords;
    }

    private RecordConsumerSupport() {}
}
