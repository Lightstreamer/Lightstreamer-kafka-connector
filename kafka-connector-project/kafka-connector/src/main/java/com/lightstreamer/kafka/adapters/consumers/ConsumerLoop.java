
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

package com.lightstreamer.kafka.adapters.consumers;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    private final MetadataListener metadataListener;
    private final ItemEventListener eventListener;
    private final ReentrantLock consumerLock = new ReentrantLock();
    private volatile ConsumerWrapper<K, V> consumer;
    private AtomicBoolean infoLock = new AtomicBoolean(false);

    private final ExecutorService pool;

    public ConsumerLoop(
            ConsumerLoopConfig<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener) {
        super(config);
        this.metadataListener = metadataListener;
        this.eventListener = eventListener;
        this.pool = Executors.newFixedThreadPool(2);
    }

    @Override
    void startConsuming() throws SubscriptionException {
        log.atTrace().log("Acquiring consumer lock...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired...");
        try {
            consumer = new ConsumerWrapper<>(config, eventListener, subscribedItems.values());
            CompletableFuture.runAsync(consumer, pool);
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
            metadataListener.forceUnsubscriptionAll();
        } finally {
            log.atTrace().log("Releasing consumer lock...");
            consumerLock.unlock();
            log.atTrace().log("Released consumer lock");
        }
    }

    @Override
    void stopConsuming() {
        log.atDebug().log("No more subscribed items");
        log.atTrace().log("Acquiring consumer lock to stop consuming...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired to stop consuming...");
        try {
            if (consumer != null) {
                log.atDebug().log("Stopping consumer...");
                consumer.close();
                log.atDebug().log("Stopped consumer");
            } else {
                log.atDebug().log("Consumer not yet started");
            }
        } finally {
            log.atTrace().log("Releasing consumer lock to stop consuming");
            consumerLock.unlock();
            log.atTrace().log("Releases consumer lock to stop consuming");
        }
    }

    @Override
    public void unsubscribeInfoItem() {
        while (!infoLock.compareAndSet(false, true)) {
            this.infoItemhande = null;
        }
    }

    public interface AdminInterface extends AutoCloseable {

        Set<String> listTopics(ListTopicsOptions options) throws Exception;

        static AdminInterface newAdmin(Properties properties) {
            return new DefaultAdminInterface(properties);
        }
    }

    static class DefaultAdminInterface implements AdminInterface {

        private final Admin admin;

        DefaultAdminInterface(Properties properties) {
            admin = Admin.create(properties);
        }

        @Override
        public Set<String> listTopics(ListTopicsOptions options) throws Exception {
            return admin.listTopics(options).names().get();
        }

        @Override
        public void close() throws Exception {
            admin.close();
        }
    }

    interface RecordProcessor<K, V> {

        void process(ConsumerRecord<K, V> record) throws ValueException;
    }

    static class DefaultRecordProcessor<K, V> implements RecordProcessor<K, V> {

        private final RecordMapper<K, V> recordMapper;
        private final ItemTemplates<K, V> templates;
        private final DataExtractor<K, V> fieldsExtractor;
        private final Collection<SubscribedItem> items;
        private final ItemEventListener listener;
        private final Logger log;

        DefaultRecordProcessor(
                RecordMapper<K, V> recordMapper,
                ItemTemplates<K, V> templates,
                DataExtractor<K, V> fieldsExtractor,
                Collection<SubscribedItem> subscribedItems,
                ItemEventListener listener,
                Logger log) {
            this.recordMapper = recordMapper;
            this.templates = templates;
            this.fieldsExtractor = fieldsExtractor;
            this.items = subscribedItems;
            this.listener = listener;
            this.log = log;
        }

        @Override
        public void process(ConsumerRecord<K, V> record) throws ValueException {
            log.atDebug().log("Mapping incoming Kafka record");
            log.atTrace().log("Kafka record: {}", record.toString());

            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));

            // Logging the mapped record is expensive, log lazly it only at trace level.
            log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
            log.atDebug().log("Mapped Kafka record");

            Set<SubscribedItem> routables = templates.routes(mappedRecord, items);

            log.atDebug().log("Filtering updates");
            Map<String, String> updates = mappedRecord.filter(fieldsExtractor);

            for (SubscribedItem sub : routables) {
                log.atDebug().log("Sending updates: {}", updates);
                listener.smartUpdate(sub.itemHandle(), updates, false);
            }
        }
    }

    interface RecordConsumer<K, V> {

        enum OrederStrategy {
            ORDER_BY_KEY(record -> Objects.toString(record.key(), null)),
            ORDER_BY_PARTITION(record -> String.valueOf(record.partition())),
            UNORDERD(record -> null);

            private Function<ConsumerRecord<?, ?>, String> sequence;

            OrederStrategy(Function<ConsumerRecord<?, ?>, String> sequence) {
                this.sequence = sequence;
            }

            String getSequence(ConsumerRecord<?, ?> record) {
                return sequence.apply(record);
            }
        }

        default void consume(ConsumerRecords<K, V> records) {
            records.forEach(this::consumeRecord);
        }

        void consumeRecord(ConsumerRecord<K, V> record);

        default void close() {}

        static class Builder<K, V> {

            private OffsetService offsetService;
            private RecordProcessor<K, V> processor;
            private Logger logger;
            private RecordErrorHandlingStrategy errorStrategy;
            private int threads;
            private OrederStrategy orderStrategy = OrederStrategy.ORDER_BY_KEY;
            private boolean prefereSingleThread = false;

            Builder<K, V> offsetService(OffsetService offsetService) {
                this.offsetService = offsetService;
                return this;
            }

            Builder<K, V> processor(RecordProcessor<K, V> processor) {
                this.processor = processor;
                return this;
            }

            Builder<K, V> logger(Logger logger) {
                this.logger = logger;
                return this;
            }

            Builder<K, V> errorStrategy(RecordErrorHandlingStrategy errorStrategy) {
                this.errorStrategy = errorStrategy;
                return this;
            }

            Builder<K, V> threads(int threads, OrederStrategy orederStrategy) {
                this.threads = threads;
                this.orderStrategy = orederStrategy;
                return this;
            }

            Builder<K, V> threads(int threads) {
                return threads(threads, OrederStrategy.ORDER_BY_KEY);
            }

            Builder<K, V> prefereSingleThread(boolean prefereSingleThread) {
                this.prefereSingleThread = prefereSingleThread;
                return this;
            }

            RecordConsumer<K, V> build() {
                Objects.requireNonNull(offsetService);
                Objects.requireNonNull(processor);
                Objects.requireNonNull(logger);
                Objects.requireNonNull(errorStrategy);
                if (threads < 1) {
                    throw new IllegalArgumentException("Threads number must be greater than one");
                }
                if (threads == 1) {
                    if (prefereSingleThread) {
                        return new SingleThreadedRecordConsumer<>(this);
                    }
                }
                return new ParallelRecordConsumer<>(this, this.orderStrategy);
            }
        }
    }

    abstract static class AbstractRecordConsumer<K, V> implements RecordConsumer<K, V> {

        protected final OffsetService offsetManager;
        protected final RecordProcessor<K, V> processor;
        protected final RecordErrorHandlingStrategy errorStrategy;
        protected Logger log;

        AbstractRecordConsumer(Builder<K, V> builder) {
            this.errorStrategy = builder.errorStrategy;
            this.offsetManager = builder.offsetService;
            this.processor = builder.processor;
            this.log = builder.logger;
        }
    }

    static class SingleThreadedRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        SingleThreadedRecordConsumer(Builder<K, V> builder) {
            super(builder);
        }

        @Override
        public void consume(ConsumerRecords<K, V> records) {
            records.forEach(this::consumeRecord);
            // offsetManager.commitAsync();
            // assert (offsetManager.getFirstFailure() == null);
        }

        @Override
        public void consumeRecord(ConsumerRecord<K, V> record) {
            try {
                processor.process(record);
                offsetManager.updateOffsets(record);
            } catch (ValueException ve) {
                log.atWarn().log("Error while extracting record: {}", ve.getMessage());
                log.atWarn().log("Applying the {} strategy", errorStrategy);

                switch (errorStrategy) {
                    case IGNORE_AND_CONTINUE -> {
                        log.atWarn().log("Ignoring error");
                        offsetManager.updateOffsets(record);
                    }

                    case FORCE_UNSUBSCRIPTION -> {
                        log.atWarn().log("Forcing unsubscription");
                        throw new KafkaException(ve);
                    }
                }
            }
        }
    }

    static class ParallelRecordConsumer<K, V> extends AbstractRecordConsumer<K, V> {

        private final Multiplexer multiplexer;
        private OrederStrategy orderStrategy;

        ParallelRecordConsumer(Builder<K, V> builder, OrederStrategy orederStrategy) {
            super(builder);
            orderStrategy = orederStrategy;

            AtomicInteger threadCount = new AtomicInteger();
            ExecutorService executor =
                    Executors.newFixedThreadPool(
                            builder.threads,
                            r ->
                                    new Thread(
                                            r,
                                            "ParallelConsumer-" + threadCount.getAndIncrement()));
            boolean asyncProcessing = false; // TODO config.isAsyncProcessing();
            // if (asyncProcessing
            //         && config.recordErrorHandlingStrategy()
            //                 != RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE) {
            //     throw new KafkaException(
            //             config.recordErrorHandlingStrategy()
            //                     + " error strategy not supported with pure asynchronous
            // processing");
            // }
            boolean batchSupport = !asyncProcessing;
            multiplexer = new Multiplexer(executor, batchSupport);
        }

        @Override
        public void close() {
            multiplexer.shutdown();
        }

        @Override
        public void consume(ConsumerRecords<K, V> records) {
            records.forEach(this::consumeRecord);
            if (multiplexer.supportsBatching()) {
                multiplexer.waitBatch();
                // NOTE: this may be inefficient if, for instance, a single task
                // should keep the executor engaged while all other threads are idle,
                // but this is not expected when all tasks are short and cpu-bound
                // offsetManager.commitAsync();
                // ValueException ve = offsetManager.getFirstFailure();
                // if (ve != null) {
                //     log.atWarn().log("Forcing unsubscription");
                //     throw new KafkaException(ve);
                // }
            }
        }

        @Override
        public void consumeRecord(ConsumerRecord<K, V> record) {
            // offsetManager.updateOffsets(record);
            String sequenceKey = orderStrategy.getSequence(record);
            multiplexer.execute(sequenceKey, () -> process(record));
        }

        void process(ConsumerRecord<K, V> record) {
            try {
                processor.process(record);
            } catch (ValueException ve) {
                log.atWarn().log("Error while extracting record: {}", ve.getMessage());
                log.atWarn().log("Applying the {} strategy", errorStrategy);
                handleError(record, ve);
            } catch (RuntimeException | Error e) {
                // TODO uguale a ValueException ?
            }
        }

        private void handleError(ConsumerRecord<K, V> record, ValueException ve) {
            switch (errorStrategy) {
                case IGNORE_AND_CONTINUE -> {
                    log.atWarn().log("Ignoring error");
                }

                case FORCE_UNSUBSCRIPTION -> {
                    // Trying to emulate the behavior of the above synchronous
                    // case,
                    // whereas the first record which gets an error ends the
                    // commits;
                    // hence we will keep track of the first error found on each
                    // partition;
                    // then, when committing each partition after the
                    // termination of the current poll,
                    // we will end before the first failed record found;
                    // this, obviously, is not possible in the fire-and-forget
                    // policy, but requires the batching one.
                    //
                    // There is a difference, though:
                    // in the synchronous case, the first error also stops the
                    // elaboration on the whole topic
                    // and all partitions are committed to the current point;
                    // in this case, instead, the elaboration continues until
                    // the end of the poll,
                    // hence, partitions with errors are committed up to the
                    // first error,
                    // but subsequent records, though not committed, may have
                    // been processed all the same
                    // (even records with the same key of a previously failed
                    // record)
                    // and only then is the elaboration stopped;
                    // on the other hand, partitions without errors are
                    // processed and committed entirely,
                    // which is good, although, then, the elaboration stops also
                    // for them.
                    assert (multiplexer.supportsBatching());
                    log.atWarn().log("Will force unsubscription");
                    offsetManager.onAsyncFailure(record, ve);
                }
            }
        }
    }

    static class ConsumerWrapper<K, V> implements Runnable {

        private static final Duration POLL_DURATION = Duration.ofMillis(Long.MAX_VALUE);

        private final ConsumerLoopConfig<K, V> config;
        private final Logger log;
        private final RecordMapper<K, V> recordMapper;
        private final Consumer<K, V> consumer;
        private final OffsetService offsetService;
        private Thread hook;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Function<Properties, AdminInterface> adminFactory;
        private final RecordConsumer<K, V> recordConsumer;

        ConsumerWrapper(
                ConsumerLoopConfig<K, V> config,
                ItemEventListener eventListener,
                Collection<SubscribedItem> subscribedItems)
                throws KafkaException {
            this(
                    config,
                    () ->
                            new KafkaConsumer<>(
                                    config.consumerProperties(),
                                    config.keyDeserializer(),
                                    config.valueDeserializer()),
                    props -> AdminInterface.newAdmin(props),
                    eventListener,
                    subscribedItems,
                    0);
        }

        ConsumerWrapper(
                ConsumerLoopConfig<K, V> config,
                Supplier<Consumer<K, V>> consumerSupplier,
                Function<Properties, AdminInterface> admin,
                ItemEventListener eventListener,
                Collection<SubscribedItem> subscribedItems,
                int threads)
                throws KafkaException {
            this.config = config;
            this.log = LogFactory.getLogger(config.connectionName());
            this.adminFactory = admin;
            this.recordMapper =
                    RecordMapper.<K, V>builder()
                            .withExtractor(config.itemTemplates().extractors())
                            .withExtractor(config.fieldsExtractor())
                            .build();
            String bootStrapServers =
                    config.consumerProperties()
                            .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

            log.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

            // Instantiate the Kafka Consumer
            consumer = consumerSupplier.get();
            log.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);

            this.offsetService = new OffsetManager(consumer, log);

            // Make a new instance of RecordConsumer, single-threaded or parallel on the based of
            // the configured number  of threads.
            this.recordConsumer =
                    new RecordConsumer.Builder<K, V>()
                            .errorStrategy(config.recordErrorHandlingStrategy())
                            .offsetService(offsetService)
                            .processor(
                                    new DefaultRecordProcessor<>(
                                            recordMapper,
                                            config.itemTemplates(),
                                            config.fieldsExtractor(),
                                            subscribedItems,
                                            eventListener,
                                            log))
                            .logger(log)
                            .build();
        }

        private Thread setShutdownHook() {
            Runnable shutdownTask =
                    () -> {
                        log.atInfo().log("Invoked shutdown hook");
                        shutdown();
                    };

            Thread hook = new Thread(shutdownTask);
            Runtime.getRuntime().addShutdownHook(hook);
            return hook;
        }

        @Override
        public void run() {
            // Install the shutdown hook
            this.hook = setShutdownHook();
            log.atDebug().log("Set shutdown kook");
            try {
                if (subscribed()) {
                    poll();
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                recordConsumer.close();
                offsetService.commitSync();
                consumer.close();
                latch.countDown();
                log.atDebug().log("Kafka Consumer closed");
            }
        }

        private boolean subscribed() {
            // Original requested topics.
            Set<String> topics = new HashSet<>(config.itemTemplates().topics());
            log.atInfo().log("Subscribing to requested topics [{}]", topics);
            log.atDebug().log("Checking existing topics on Kafka");

            // Check the actual available topics on Kafka.
            try (AdminInterface admin = adminFactory.apply(config.consumerProperties())) {
                ListTopicsOptions options = new ListTopicsOptions();
                options.timeoutMs(30000);

                // Retain from the original requestes topics the available ones.
                Set<String> existingTopics = admin.listTopics(options);
                boolean notAllPresent = topics.retainAll(existingTopics);

                // Can't subscribe at all. Force unsubscription and exit the loop.
                if (topics.isEmpty()) {
                    log.atWarn().log("Not found requested topics");
                    // metadataListener.forceUnsubscriptionAll();
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
                // metadataListener.forceUnsubscriptionAll();
                return false;
            }
        }

        private void poll() throws WakeupException {
            while (true) {
                log.atInfo().log("Polling records");
                try {
                    ConsumerRecords<K, V> records = consumer.poll(POLL_DURATION);
                    System.out.println("Consuming " + records.count() + " records");
                    log.atDebug().log("Received records");
                    consumeRecords(records);
                    log.atInfo().log("Consumed {} records", records.count());
                } catch (WakeupException we) {
                    // Catch and rethrow the Exception here because of the next KafkaException
                    throw we;
                } catch (KafkaException ke) {
                    log.atError().setCause(ke).log("Unrecoverable exception");
                    // metadataListener.forceUnsubscriptionAll();
                    break;
                }
            }
        }

        void consumeRecords(ConsumerRecords<K, V> records) {
            recordConsumer.consume(records);
        }

        // void consumeRecords(ConsumerRecords<K, V> records) {
        //     records.forEach(this::consume);
        //     if (multiplexer != null && multiplexer.supportsBatching()) {
        //         // multiplexer.waitBatch();
        //         // NOTE: this may be inefficient if, for instance, a single task
        //         // should keep the executor engaged while all other threads are idle,
        //         // but this is not expected when all tasks are short and cpu-bound
        //         offsetManager.commitAsync();
        //         ValueException ve = offsetManager.getFirstFailure();
        //         if (ve != null) {
        //             log.atWarn().log("Forcing unsubscription");
        //             throw new KafkaException(ve);
        //         }
        //     } else {
        //         offsetManager.commitAsync();
        //         assert (offsetManager.getFirstFailure() == null);
        //     }
        // }

        // protected void consume(ConsumerRecord<K, V> record) {
        //     log.atDebug().log("Consuming Kafka record");

        //     if (multiplexer == null) {
        //         try {
        //             process(record);
        //             offsetManager.updateOffsets(record);

        //         } catch (ValueException ve) {
        //             log.atWarn().log("Error while extracting record: {}", ve.getMessage());
        //             log.atWarn()
        //                     .log("Applying the {} strategy",
        // config.recordErrorHandlingStrategy());

        //             switch (config.recordErrorHandlingStrategy()) {
        //                 case IGNORE_AND_CONTINUE -> {
        //                     log.atWarn().log("Ignoring error");
        //                     offsetManager.updateOffsets(record);
        //                 }

        //                 case FORCE_UNSUBSCRIPTION -> {
        //                     log.atWarn().log("Forcing unsubscription");
        //                     throw new KafkaException(ve);
        //                 }
        //             }
        //         }
        //     } else {
        //         offsetManager.updateOffsets(record);
        //         String sequenceKey = Objects.toString(record.key(), null);
        //         multiplexer.execute(
        //                 sequenceKey,
        //                 () -> {
        //                     try {
        //                         process(record);
        //                     } catch (ValueException ve) {
        //                         log.atWarn()
        //                                 .log("Error while extracting record: {}",
        // ve.getMessage());
        //                         log.atWarn()
        //                                 .log(
        //                                         "Applying the {} strategy",
        //                                         config.recordErrorHandlingStrategy());

        //                         switch (config.recordErrorHandlingStrategy()) {
        //                             case IGNORE_AND_CONTINUE -> {
        //                                 log.atWarn().log("Ignoring error");
        //                             }

        //                             case FORCE_UNSUBSCRIPTION -> {
        //                                 // Trying to emulate the behavior of the above
        // synchronous
        //                                 // case,
        //                                 // whereas the first record which gets an error ends the
        //                                 // commits;
        //                                 // hence we will keep track of the first error found on
        // each
        //                                 // partition;
        //                                 // then, when committing each partition after the
        //                                 // termination of the current poll,
        //                                 // we will end before the first failed record found;
        //                                 // this, obviously, is not possible in the
        // fire-and-forget
        //                                 // policy, but requires the batching one.
        //                                 //
        //                                 // There is a difference, though:
        //                                 // in the synchronous case, the first error also stops
        // the
        //                                 // elaboration on the whole topic
        //                                 // and all partitions are committed to the current point;
        //                                 // in this case, instead, the elaboration continues until
        //                                 // the end of the poll,
        //                                 // hence, partitions with errors are committed up to the
        //                                 // first error,
        //                                 // but subsequent records, though not committed, may have
        //                                 // been processed all the same
        //                                 // (even records with the same key of a previously failed
        //                                 // record)
        //                                 // and only then is the elaboration stopped;
        //                                 // on the other hand, partitions without errors are
        //                                 // processed and committed entirely,
        //                                 // which is good, although, then, the elaboration stops
        // also
        //                                 // for them.
        //                                 assert (multiplexer.supportsBatching());
        //                                 log.atWarn().log("Will force unsubscription");
        //                                 offsetManager.onAsyncFailure(record, ve);
        //                             }
        //                         }
        //                     } catch (RuntimeException | Error e) {
        //                         // TODO uguale a ValueException ?
        //                     }
        //                 });
        //     }
        // }

        // protected void process(ConsumerRecord<K, V> record) throws ValueException {
        //     log.atDebug().log("Mapping incoming Kafka record");
        //     log.atTrace().log("Kafka record: {}", record.toString());

        //     MappedRecord mappedRecord = recordRemapper.map(KafkaRecord.from(record));

        //     // Logging the mapped record is expensive, log lazly it only at trace level.
        //     log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
        //     log.atDebug().log("Mapped Kafka record");

        //     Set<SubscribedItem> routables =
        //             config.itemTemplates().routes(mappedRecord, subscribedItems);

        //     log.atDebug().log("Filtering updates");
        //     Map<String, String> updates = mappedRecord.filter(config.fieldsExtractor());

        //     for (SubscribedItem sub : routables) {
        //         log.atDebug().log("Sending updates: {}", updates);
        //         listener.smartUpdate(sub.itemHandle(), updates, false);
        //     }
        // }

        void close() {
            shutdown();
            if (this.hook != null) {
                Runtime.getRuntime().removeShutdownHook(this.hook);
            }
        }

        private void shutdown() {
            log.atInfo().log("Shutting down Kafka consumer");
            log.atDebug().log("Waking up consumer");
            consumer.wakeup();
            log.atDebug().log("Consumer woken up");
            try {
                log.atTrace().log("Waiting for graceful thread completion");
                latch.await();
                log.atTrace().log("Completed thread");
            } catch (InterruptedException e) {
                // Ignore
            }
            log.atInfo().log("Shut down Kafka consumer");
        }
    }

    public interface OffsetService extends ConsumerRebalanceListener {

        void commitSync();

        void commitAsync();

        void updateOffsets(ConsumerRecord<?, ?> record);

        void onAsyncFailure(ConsumerRecord<?, ?> record, ValueException ve);

        ValueException getFirstFailure();
    }

    static class OffsetManager implements OffsetService {

        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        private final Map<TopicPartition, OffsetAndMetadata> currentFailedOffsets =
                new ConcurrentHashMap<>();
        private volatile ValueException firstFailure = null;
        private final Consumer<?, ?> consumer;
        private final Logger log;

        OffsetManager(Consumer<?, ?> consumer, Logger logger) {
            this.consumer = consumer;
            this.log = logger;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.atWarn().log("Partions revoked, committing offsets {}", currentOffsets);
            commitSync();
        }

        @Override
        public void commitSync() {
            integrateFailures();
            consumer.commitSync(currentOffsets);
            log.atInfo().log("Offsets commited");
        }

        @Override
        public void commitAsync() {
            integrateFailures();
            consumer.commitAsync(currentOffsets, null);
        }

        private void integrateFailures() {
            currentFailedOffsets.forEach(
                    (p, o) -> {
                        assert (currentOffsets.containsKey(o));
                        currentOffsets.put(p, o);
                        // if we reach this point, more invocations of updateOffsets will not be
                        // expected and supported
                    });
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.atDebug().log("Assigned partiions {}", partitions);
        }

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {
            currentOffsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1, null));
        }

        @Override
        public void onAsyncFailure(ConsumerRecord<?, ?> record, ValueException ve) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata offset =
                    new OffsetAndMetadata(
                            record.offset(), null); // this record should not be committed
            currentFailedOffsets.compute(
                    partition, (p, o) -> o == null || o.offset() > offset.offset() ? offset : o);
            if (firstFailure == null) {
                firstFailure = ve; // any of the first exceptions got should be enough
            }
        }

        public ValueException getFirstFailure() {
            return firstFailure;
        }
    }
}
