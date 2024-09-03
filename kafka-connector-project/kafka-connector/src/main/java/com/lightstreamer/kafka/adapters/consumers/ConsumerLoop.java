
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

        ConsumerWrapper(RecordErrorHandlingStrategy errorStrategy) throws KafkaException {
            this.errorStrategy = errorStrategy;
            String bootStrapServers =
                    config.consumerProperties()
                            .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            String bootStrapServers =
                    config.consumerProperties()
                            .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            log.atInfo().log("Starting connection to Kafka broker(s) at {}", bootStrapServers);

            consumer =
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            config.keyDeserializer(),
                            config.valueDeserializer());
            consumer =
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            config.keyDeserializer(),
                            config.valueDeserializer());
            log.atInfo().log("Established connection to Kafka broker(s) at {}", bootStrapServers);

            this.offsetManager = new OffsetManager(consumer);
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
                    offsetManager.commitAsync();
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
            log.atDebug().log("Mapping incoming Kafka record");
            log.atTrace().log("Kafka record: {}", record.toString());
            try {
                MappedRecord mappedRecord = recordRemapper.map(KafkaRecord.from(record));

                // Logging the mapped record is expensive, log lazly it only at trace level.
                log.atTrace().log(() -> "Mapped Kafka record to %s".formatted(mappedRecord));
                log.atDebug().log("Mapped Kafka record");

                Set<SubscribedItem> routables =
                        config.itemTemplates().routes(mappedRecord, subscribedItems.values());

                log.atDebug().log("Filtering updates");
                Map<String, String> updates = mappedRecord.filter(fieldsExtractor);

                log.atInfo().log("Routing record to {} items", routables.size());
                for (SubscribedItem sub : routables) {
                    log.atDebug().log("Sending updates: {}", updates);
                    eventListener.smartUpdate(sub.itemHandle(), updates, false);
                }

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
            consumer.commitSync(currentOffsets);
            log.atInfo().log("Offsets commited");
        }

        void commitAsync() {
            consumer.commitAsync(currentOffsets, null);
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
    }
}
