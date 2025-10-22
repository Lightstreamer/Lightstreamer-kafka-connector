
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        public StartBuildingConsumer<K, V> eventListener(ItemEventListener listener) {
            parentBuilder.listener = Objects.requireNonNull(listener, "ItemEventListener not set");

            EventUpdater updater =
                    EventUpdater.create(
                            parentBuilder.listener,
                            !parentBuilder.subscribed.acceptSubscriptions());

            ProcessUpdatesStrategy processUpdatesStrategy =
                    ProcessUpdatesStrategy.fromCommandModeStrategy(
                            parentBuilder.commandModeStrategy);

            RecordRoutingStrategy recordRoutingStrategy =
                    RecordRoutingStrategy.fromSubscribedItems(parentBuilder.subscribed);

            return new StartBuildingConsumerImpl<>(
                    new DefaultRecordProcessor<>(
                            parentBuilder.mapper,
                            updater,
                            processUpdatesStrategy,
                            recordRoutingStrategy));
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

    public static interface EventUpdater {

        void update(SubscribedItem sub, Map<String, String> updates, boolean isSnapshot);

        void clearSnapshot(SubscribedItem sub);

        void endOfSnapshot(SubscribedItem sub);

        ItemEventListener listener();

        static EventUpdater create(ItemEventListener listener, boolean isLegacy) {
            if (isLegacy) {
                return new LegacyEventUpdater(listener);
            } else {
                return new SmartEventUpdater(listener);
            }
        }
    }

    abstract static sealed class AbstractEventUpdaters implements EventUpdater
            permits LegacyEventUpdater, SmartEventUpdater {

        private final ItemEventListener listener;

        AbstractEventUpdaters(ItemEventListener listener) {
            this.listener = listener;
        }

        @Override
        public final ItemEventListener listener() {
            return listener;
        }
    }

    static final class LegacyEventUpdater extends AbstractEventUpdaters {

        LegacyEventUpdater(ItemEventListener listener) {
            super(listener);
        }

        public void update(SubscribedItem sub, Map<String, String> updates, boolean isSnapshot) {
            listener().update(sub.asCanonicalItemName(), updates, isSnapshot);
        }

        public void clearSnapshot(SubscribedItem sub) {
            listener().clearSnapshot(sub.asCanonicalItemName());
        }

        public void endOfSnapshot(SubscribedItem sub) {
            listener().endOfSnapshot(sub.asCanonicalItemName());
        }
    }

    static final class SmartEventUpdater extends AbstractEventUpdaters {

        SmartEventUpdater(ItemEventListener listener) {
            super(listener);
        }

        public void update(SubscribedItem sub, Map<String, String> updates, boolean isSnapshot) {
            listener().smartUpdate(sub.itemHandle(), updates, isSnapshot);
        }

        public void clearSnapshot(SubscribedItem sub) {
            listener().smartClearSnapshot(sub.itemHandle());
        }

        public void endOfSnapshot(SubscribedItem sub) {
            listener().smartEndOfSnapshot(sub.itemHandle());
        }
    }

    static interface RecordRoutingStrategy {

        Set<SubscribedItem> route(MappedRecord mappedRecord);

        default boolean canRouteImplicitItems() {
            return false;
        }

        static RecordRoutingStrategy fromSubscribedItems(SubscribedItems subscribedItems) {
            return subscribedItems.acceptSubscriptions()
                    ? new DefaultRoutingStrategy(subscribedItems)
                    : new RouteAllStrategy();
        }
    }

    static class DefaultRoutingStrategy implements RecordRoutingStrategy {

        protected final SubscribedItems subscribedItems;

        DefaultRoutingStrategy(SubscribedItems subscribedItems) {
            this.subscribedItems = subscribedItems;
        }

        @Override
        public Set<SubscribedItem> route(MappedRecord mappedRecord) {
            return mappedRecord.route(subscribedItems);
        }
    }

    static class RouteAllStrategy implements RecordRoutingStrategy {

        @Override
        public Set<SubscribedItem> route(MappedRecord mappedRecord) {
            return mappedRecord.routeAll();
        }

        @Override
        public boolean canRouteImplicitItems() {
            return true;
        }
    }

    static class ComposedRoutingStrategy implements RecordRoutingStrategy {

        private final RecordRoutingStrategy[] strategies;

        ComposedRoutingStrategy(RecordRoutingStrategy... strategies) {
            this.strategies = strategies;
        }

        @Override
        public Set<SubscribedItem> route(MappedRecord mappedRecord) {
            Set<SubscribedItem> routedItems = new HashSet<>();
            for (RecordRoutingStrategy strategy : strategies) {
                routedItems.addAll(strategy.route(mappedRecord));
            }

            return routedItems;
        }

        @Override
        public boolean canRouteImplicitItems() {
            for (RecordRoutingStrategy strategy : strategies) {
                if (strategy.canRouteImplicitItems()) {
                    return true;
                }
            }
            return false;
        }
    }

    static interface ProcessUpdatesStrategy {

        default void processUpdates(
                EventUpdater updater, MappedRecord record, Set<SubscribedItem> routable) {
            Map<String, String> updates = getEvent(record);
            getLogger().atDebug().log(() -> "Sending updates: %s".formatted(updates));
            doProcessUpdates(updater, updates, routable);
        }

        default Map<String, String> getEvent(MappedRecord record) {
            return record.fieldsMap();
        }

        void doProcessUpdates(
                EventUpdater updater, Map<String, String> updates, Set<SubscribedItem> routable);

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

        private Logger log = LoggerFactory.getLogger(ProcessUpdatesStrategy.class);

        public void doProcessUpdates(
                EventUpdater updater, Map<String, String> updates, Set<SubscribedItem> routable) {
            for (SubscribedItem sub : routable) {
                updater.update(sub, updates, false);
            }
        }

        public final void useLogger(Logger logger) {
            this.log = Objects.requireNonNullElse(logger, this.log);
        }

        @Override
        public Logger getLogger() {
            return log;
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

        static String SNAPSHOT = "snapshot";

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
        public void doProcessUpdates(
                EventUpdater updater, Map<String, String> updates, Set<SubscribedItem> routable) {
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
                    handleSnapshot(updater, cmd, sub);
                } else {
                    getLogger().atDebug().log(() -> "Sending %s command".formatted(cmd.toString()));
                    updater.update(sub, updates, sub.isSnapshot());
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

        private void handleSnapshot(EventUpdater updater, Command snapshot, SubscribedItem sub) {
            switch (snapshot) {
                case CS -> {
                    getLogger().atDebug().log("Sending clearSnapshot");
                    updater.clearSnapshot(sub);
                    sub.setSnapshot(true);
                }
                case EOS -> {
                    getLogger().atDebug().log("Sending endOfSnapshot");
                    updater.endOfSnapshot(sub);
                    sub.setSnapshot(false);
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
        protected final RecordRoutingStrategy recordRoutingStrategy;
        protected final EventUpdater updater;
        protected Logger log = LoggerFactory.getLogger(DefaultRecordProcessor.class);

        DefaultRecordProcessor(
                RecordMapper<K, V> recordMapper,
                EventUpdater updater,
                ProcessUpdatesStrategy processUpdatesStrategy,
                RecordRoutingStrategy recordRoutingStrategy) {
            this.recordMapper = recordMapper;
            this.updater = updater;
            this.processUpdatesStrategy = processUpdatesStrategy;
            this.recordRoutingStrategy = recordRoutingStrategy;
        }

        @Override
        public final void useLogger(Logger logger) {
            this.log = Objects.requireNonNullElse(logger, this.log);
            this.processUpdatesStrategy.useLogger(logger);
        }

        @Override
        public final void process(ConsumerRecord<K, V> record) throws ValueException {
            log.atDebug().log(() -> "Mapping incoming Kafka record");
            log.atTrace().log(() -> "Kafka record: %s".formatted(record.toString()));

            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));
            // As logging the mapped record is expensive, log lazily it only at trace level.
            log.atTrace().log(() -> "Kafka record mapped to %s".formatted(mappedRecord));
            log.atDebug().log(() -> "Kafka record mapped");

            route(mappedRecord);
        }

        private void route(MappedRecord mappedRecord) {
            Set<SubscribedItem> routable = recordRoutingStrategy.route(mappedRecord);
            if (routable.size() > 0) {
                log.atDebug().log(() -> "Routing record to %d items".formatted(routable.size()));
                processUpdatesStrategy.processUpdates(updater, mappedRecord, routable);
            } else {
                log.atDebug().log("No routable items found");
            }
        }

        @Override
        public ProcessUpdatesType processUpdatesType() {
            return processUpdatesStrategy.type();
        }

        @Override
        public boolean canRouteImplicitItems() {
            return recordRoutingStrategy.canRouteImplicitItems();
        }
    }

    private abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected final OffsetService offsetService;
        protected final RecordProcessor<K, V> recordProcessor;
        protected final Logger logger;
        private final RecordErrorHandlingStrategy errorStrategy;
        private volatile boolean terminated = false;

        AbstractRecordConsumer(StartBuildingConsumerImpl<K, V> builder) {
            this.errorStrategy = builder.errorStrategy;
            this.offsetService = builder.offsetService;
            this.logger = builder.logger;
            this.recordProcessor = builder.processor;
            // Enforce usage of the same logger
            this.recordProcessor.useLogger(logger);
        }

        @Override
        public final void terminate() {
            offsetService.commitSyncAndIgnoreErrors();
            onTermination();
            terminated = true;
        }

        @Override
        public boolean isTerminated() {
            return terminated;
        }

        void onTermination() {}

        @Override
        public final RecordErrorHandlingStrategy errorStrategy() {
            return errorStrategy;
        }

        @Override
        public final RecordProcessor<K, V> recordProcessor() {
            return recordProcessor;
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

            this.taskExecutor =
                    TaskExecutor.create(
                            newFixedThreadPool(
                                    actualThreads, r -> newThread(r, new AtomicInteger())));
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
        void onTermination() {
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
