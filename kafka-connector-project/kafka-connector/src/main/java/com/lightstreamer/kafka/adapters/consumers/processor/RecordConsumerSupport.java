
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
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WihtOffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithLogger;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithOptionals;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.WithSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        protected boolean prefereSingleThread = false;

        StartBuildingConsumerImpl(RecordProcessor<K, V> processor) {
            this.processor = processor;
        }

        @Override
        public WihtOffsetService<K, V> offsetService(OffsetService offsetService) {
            this.offsetService = offsetService;
            return new WithOffsetServiceImpl<>(this);
        }
    }

    private static class StartBuildingProcessorBuilderImpl<K, V>
            implements StartBuildingProcessor<K, V> {

        protected RecordMapper<K, V> mapper;
        protected Collection<SubscribedItem> subscribed;
        protected ItemEventListener listener;

        StartBuildingProcessorBuilderImpl(RecordMapper<K, V> mapper) {
            this.mapper = mapper;
        }

        @Override
        public WithSubscribedItems<K, V> subscribedItems(
                Collection<SubscribedItem> subscribedItems) {
            this.subscribed = subscribedItems;
            return new WithSubscribedItemsImpl<>(this);
        }
    }

    private static class WithSubscribedItemsImpl<K, V> implements WithSubscribedItems<K, V> {
        final StartBuildingProcessorBuilderImpl<K, V> parentBuilder;

        WithSubscribedItemsImpl(StartBuildingProcessorBuilderImpl<K, V> b) {
            this.parentBuilder = b;
        }

        @Override
        public StartBuildingConsumer<K, V> eventListener(ItemEventListener listener) {
            this.parentBuilder.listener = listener;
            Objects.requireNonNull(this.parentBuilder.mapper);
            Objects.requireNonNull(this.parentBuilder.subscribed);
            Objects.requireNonNull(this.parentBuilder.listener);
            RecordProcessor<K, V> recordProcessor =
                    new DefaultRecordProcessor<>(
                            this.parentBuilder.mapper,
                            this.parentBuilder.subscribed,
                            this.parentBuilder.listener);
            return new StartBuildingConsumerImpl<>(recordProcessor);
        }
    }

    private static class WithOffsetServiceImpl<K, V> implements WihtOffsetService<K, V> {

        final StartBuildingConsumerImpl<K, V> parentBuilder;

        WithOffsetServiceImpl(StartBuildingConsumerImpl<K, V> b) {
            this.parentBuilder = b;
        }

        @Override
        public WithLogger<K, V> errorStrategy(RecordErrorHandlingStrategy stragey) {
            this.parentBuilder.errorStrategy = stragey;
            return new WithLoggerImpl<>(parentBuilder);
        }
    }

    private static class WithLoggerImpl<K, V> implements WithLogger<K, V> {

        final StartBuildingConsumerImpl<K, V> b;

        WithLoggerImpl(StartBuildingConsumerImpl<K, V> b) {
            this.b = b;
        }

        @Override
        public WithOptionals<K, V> logger(Logger logger) {
            this.b.logger = logger;
            return new WithOptionalsImpl<>(b);
        }
    }

    private static class WithOptionalsImpl<K, V> implements WithOptionals<K, V> {
        final StartBuildingConsumerImpl<K, V> parentBuilder;

        WithOptionalsImpl(StartBuildingConsumerImpl<K, V> b) {
            this.parentBuilder = b;
        }

        @Override
        public WithOptionals<K, V> threads(int threads) {
            this.parentBuilder.threads = threads;
            return this;
        }

        @Override
        public WithOptionals<K, V> ordering(OrderStrategy orderStrategy) {
            this.parentBuilder.orderStrategy = orderStrategy;
            return this;
        }

        @Override
        public WithOptionals<K, V> preferSingleThread(boolean singleThread) {
            this.parentBuilder.prefereSingleThread = singleThread;
            return this;
        }

        @Override
        public RecordConsumer<K, V> build() {
            Objects.requireNonNull(parentBuilder.errorStrategy);
            Objects.requireNonNull(parentBuilder.logger);
            Objects.requireNonNull(parentBuilder.orderStrategy);
            if (parentBuilder.threads < 1 && parentBuilder.threads != -1) {
                throw new IllegalArgumentException("Threads number must be greater than zero");
            }
            if (parentBuilder.threads == 1 && parentBuilder.prefereSingleThread) {
                return new SingleThreadedRecordConsumer<>(parentBuilder);
            }
            return new ParallelRecordConsumer<>(parentBuilder);
        }
    }

    static class DefaultRecordProcessor<K, V> implements RecordProcessor<K, V> {

        protected final RecordMapper<K, V> recordMapper;
        protected final Collection<SubscribedItem> subscribedItems;
        protected final ItemEventListener listener;
        protected Logger log = LoggerFactory.getLogger(DefaultRecordProcessor.class);

        DefaultRecordProcessor(
                RecordMapper<K, V> recordMapper,
                Collection<SubscribedItem> subscribedItems,
                ItemEventListener listener) {
            this.recordMapper = recordMapper;
            this.subscribedItems = subscribedItems;
            this.listener = listener;
        }

        @Override
        public void useLogger(Logger logger) {
            this.log = Objects.requireNonNullElse(logger, this.log);
        }

        @Override
        public void process(ConsumerRecord<K, V> record) throws ValueException {
            log.atDebug().log(() -> "Mapping incoming Kafka record");
            log.atTrace().log(() -> "Kafka record: %s".formatted(record.toString()));

            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));

            // As logging the mapped record is expensive, log lazly it only at trace level.
            log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
            log.atDebug().log(() -> "Mapped Kafka record");

            Set<SubscribedItem> routables = mappedRecord.route(subscribedItems);
            if (routables.size() > 0) {
                log.atDebug().log(() -> "Filtering updates");
                Map<String, String> updates = mappedRecord.fieldsMap();

                log.atInfo().log("Routing record to {} items", routables.size());
                for (SubscribedItem sub : routables) {
                    log.atDebug().log(() -> "Sending updates: %s".formatted(updates));
                    listener.smartUpdate(sub.itemHandle(), updates, false);
                }
            } else {
                log.atInfo().log("No routable items found");
            }
        }
    }

    private abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected final OffsetService offsetService;
        protected final RecordProcessor<K, V> recordProcessor;
        protected final RecordErrorHandlingStrategy errorStrategy;
        protected final Logger logger;

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
            } catch (Throwable ve) {
                logger.atWarn().log("Error while extracting record: {}", ve.getMessage());
                logger.atWarn().log("Applying the {} strategy", errorStrategy);

                switch (errorStrategy) {
                    case IGNORE_AND_CONTINUE -> {
                        // We we log the error to catch the stack trace
                        logger.atWarn().setCause(ve).log("Ignoring error");
                        offsetService.updateOffsets(record);
                    }

                    case FORCE_UNSUBSCRIPTION -> {
                        // Do not log the error, which will fully logged from the consuming loop
                        logger.atWarn().log("Forcing unsubscription");
                        throw new KafkaException(ve);
                    }
                }
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
                logger.atWarn().log("Applying the {} strategy", errorStrategy);
                handleError(record, ve);
            } catch (Throwable t) {
                logger.atError().log("Serious error while processing record!");
                offsetService.onAsyncFailure(t);
            }
        }

        private void handleError(ConsumerRecord<K, V> record, ValueException ve) {
            switch (errorStrategy) {
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
        List<ConsumerRecord<K, V>> allRecords = new ArrayList<>();
        records.partitions()
                .forEach(topicPartition -> allRecords.addAll(records.records(topicPartition)));
        return allRecords;
    }

    private RecordConsumerSupport() {}
}
