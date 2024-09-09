
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
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    private final MetadataListener metadataListener;
    private final ItemEventListener eventListener;
    private final RecordMapper<K, V> recordRemapper;
    private final DataExtractor<K, V> fieldsExtractor;
    private final ReentrantLock consumerLock = new ReentrantLock();
    private volatile ConsumerWrapper consumer;
    private AtomicBoolean infoLock = new AtomicBoolean(false);

    private final ExecutorService pool;

    public ConsumerLoop(
            ConsumerLoopConfig<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener) {
        super(config);
        this.metadataListener = metadataListener;
        this.fieldsExtractor = config.fieldsExtractor();
        this.recordRemapper =
                RecordMapper.<K, V>builder()
                        .withExtractor(config.itemTemplates().extractors())
                        .withExtractor(fieldsExtractor)
                        .build();
        this.eventListener = eventListener;
        this.pool = Executors.newFixedThreadPool(2);
    }

    @Override
    void startConsuming() throws SubscriptionException {
        log.atTrace().log("Acquiring consumer lock...");
        consumerLock.lock();
        log.atTrace().log("Lock acquired...");
        try {
            consumer = new ConsumerWrapper(config.recordErrorHandlingStrategy());
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

    class ConsumerWrapper implements Runnable {

        private static final Duration POLL_DURATION = Duration.ofMillis(Long.MAX_VALUE);
        private final KafkaConsumer<K, V> consumer;
        private final OffsetManager offsetManager;
        private final OffsetManager offsetManager;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Thread hook;
        private final RecordErrorHandlingStrategy errorStrategy;

        private final Multiplexer multiplexer;

        ConsumerWrapper(RecordErrorHandlingStrategy errorStrategy) throws KafkaException {
            this.errorStrategy = errorStrategy;
            String bootStrapServers =
                    config.consumerProperties()
                            .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            log.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

            consumer =
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            config.keyDeserializer(),
                            config.valueDeserializer());
            log.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);

            Integer consumerThreads = null; // TODO config.getConsumerThreads();
            if (consumerThreads == null) {
                // no async processing at all
                multiplexer = null;
            } else {
                ExecutorService executor = Executors.newFixedThreadPool(consumerThreads);
                boolean asyncProcessing = true; // TODO config.isAsyncProcessing();
                if (asyncProcessing && errorStrategy != RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE) {
                    throw new KafkaException(errorStrategy + " error strategy not supported with pure asynchronous processing");
                }
                boolean batchSupport = !asyncProcessing;
                multiplexer = new Multiplexer(executor, batchSupport);
            }

            this.offsetManager = new OffsetManager(consumer);
            this.hook = setShutdownHook();
            log.atDebug().log("Set shutdown kook");
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
            try {
                if (subscribed()) {
                    poll();
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                offsetManager.commitSync();
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
            try (Admin admin = AdminClient.create(config.consumerProperties())) {
                ListTopicsOptions options = new ListTopicsOptions();
                options.timeoutMs(30000);
                ListTopicsResult listTopics = admin.listTopics(options);
                boolean notAllPresent = false;

                // Retain from the original requestes topics the available ones.
                Set<String> existingTopics = listTopics.names().get();
                notAllPresent = topics.retainAll(existingTopics);

                // Can't subscribe at all. Force unsubscription and exit the loop.
                if (topics.isEmpty()) {
                    log.atWarn().log("Not found requested topics");
                    metadataListener.forceUnsubscriptionAll();
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
                consumer.subscribe(topics, offsetManager);
                return true;
            } catch (Exception e) {
                log.atError().setCause(e).log();
                metadataListener.forceUnsubscriptionAll();
                return false;
            }
        }

        private void poll() throws WakeupException {
            while (true) {
                log.atInfo().log("Polling records");
                try {
                    ConsumerRecords<K, V> records = consumer.poll(POLL_DURATION);
                    log.atDebug().log("Received records");
                    records.forEach(this::consume);
                    if (multiplexer != null && multiplexer.supportsBatching()) {
                        multiplexer.waitBatch();
                        // NOTE: this may be inefficient if, for instance, a single task
                        // should keep the executor engaged while all other threads are idle,
                        // but this is not expected when all tasks are short and cpu-bound
                        offsetManager.commitAsync();
                        ValueException ve = offsetManager.getFirstFailure();
                        if (ve != null) {
                            log.atWarn().log("Forcing unsubscription");
                            throw new KafkaException(ve);
                        }
                    } else {
                        offsetManager.commitAsync();
                        assert (offsetManager.getFirstFailure() == null);
                    }
                    log.atInfo().log("Consumed {} records", records.count());
                } catch (WakeupException we) {
                    // Catch and rethrow the Exception here because of the next KafkaException
                    throw we;
                } catch (KafkaException ke) {
                    log.atError().setCause(ke).log("Unrecoverable exception");
                    metadataListener.forceUnsubscriptionAll();
                    break;
                }
            }
        }

        protected void consume(ConsumerRecord<K, V> record) {
            log.atDebug().log("Consuming Kafka record");

            if (multiplexer == null) {
                try {
                    process(record);
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
            } else {
                offsetManager.updateOffsets(record);
                String sequenceKey = Objects.toString(record.key(), null);
                multiplexer.execute(
                        sequenceKey,
                        () -> {
                            try {
                                process(record);
                            } catch (ValueException ve) {
                                log.atWarn().log("Error while extracting record: {}", ve.getMessage());
                                log.atWarn().log("Applying the {} strategy", errorStrategy);

                                switch (errorStrategy) {
                                    case IGNORE_AND_CONTINUE -> {
                                        log.atWarn().log("Ignoring error");
                                    }

                                    case FORCE_UNSUBSCRIPTION -> {
                                        // Trying to emulate the behavior of the above synchronous case,
                                        // whereas the first record which gets an error ends the commits;
                                        // hence we will keep track of the first error found on each partition;
                                        // then, when committing each partition after the termination of the current poll,
                                        // we will end before the first failed record found;
                                        // this, obviously, is not possible in the fire-and-forget policy, but requires the batching one.
                                        //
                                        // There is a difference, though:
                                        // in the synchronous case, the first error also stops the elaboration on the whole topic
                                        // and all partitions are committed to the current point;
                                        // in this case, instead, the elaboration continues until the end of the poll,
                                        // hence, partitions with errors are committed up to the first error,
                                        // but subsequent records, though not committed, may have been processed all the same
                                        // (even records with the same key of a previously failed record)
                                        // and only then is the elaboration stopped;
                                        // on the other hand, partitions without errors are processed and committed entirely,
                                        // which is good, although, then, the elaboration stops also for them.
                                        assert (multiplexer.supportsBatching());
                                        log.atWarn().log("Will force unsubscription");
                                        offsetManager.onAsyncFailure(record, ve);
                                    }
                                }
                            } catch (RuntimeException | Error e) {
                                // TODO uguale a ValueException ?
                            }
                        });
            }
        }

        protected void process(ConsumerRecord<K, V> record) throws ValueException {
            log.atDebug().log("Mapping incoming Kafka record");
            log.atTrace().log("Kafka record: {}", record.toString());

            MappedRecord mappedRecord = recordRemapper.map(KafkaRecord.from(record));

                // Logging the mapped record is expensive, log lazly it only at trace level.
                log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
                log.atDebug().log("Mapped Kafka record");

                Set<SubscribedItem> routables =
                        config.itemTemplates().routes(mappedRecord, subscribedItems.values());

            log.atDebug().log("Filtering updates");
            Map<String, String> updates = mappedRecord.filter(fieldsExtractor);

            for (SubscribedItem sub : routables) {
                log.atDebug().log("Sending updates: {}", updates);
                eventListener.smartUpdate(sub.itemHandle(), updates, false);
            }
        }

        void close() {
            shutdown();
            Runtime.getRuntime().removeShutdownHook(this.hook);
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

    class OffsetManager implements ConsumerRebalanceListener {

        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        private final Map<TopicPartition, OffsetAndMetadata> currentFailedOffsets = new ConcurrentHashMap<>();
        private volatile ValueException firstFailure = null;
        private final KafkaConsumer<?, ?> consumer;

        OffsetManager(KafkaConsumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.atWarn().log("Partions revoked, committing offsets {}", currentOffsets);
            commitSync();
        }

        void commitSync() {
            integrateFailures();
            consumer.commitSync(currentOffsets);
            log.atInfo().log("Offsets commited");
        }

        void commitAsync() {
            integrateFailures();
            consumer.commitAsync(currentOffsets, null);
        }

        private void integrateFailures() {
            currentFailedOffsets.forEach((p, o) -> {
                assert (currentOffsets.containsKey(o));
                currentOffsets.put(p, o);
                // if we reach this point, more invocations of updateOffsets will not be expected and supported
            });
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.atDebug().log("Assigned partiions {}", partitions);
        }

        void updateOffsets(ConsumerRecord<?, ?> record) {
            currentOffsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1, null));
        }

        public void onAsyncFailure(ConsumerRecord<?, ?> record, ValueException ve) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata offset = new OffsetAndMetadata(record.offset(), null); // this record should not be committed
            currentFailedOffsets.compute(partition, (p, o) -> o == null || o.offset() > offset.offset() ? offset : o);
            if (firstFailure == null) {
                firstFailure = ve; // any of the first exceptions got should be enough
            }
        }

        public ValueException getFirstFailure() {
            return firstFailure;
        }
    }
}
