
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

package com.lightstreamer.kafka_connector.adapter.consumers;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.ConsumerLoopConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.commons.MetadataListener;
import com.lightstreamer.kafka_connector.adapter.config.InfoItem;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.CommitFailedException;
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
import org.slf4j.LoggerFactory;

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    enum ValueExceptionStrategy {
        IGNORE_AND_CONTINUE,

        FORCE_UNSUBSCRIPTION;
    }

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private final ItemEventListener eventListener;
    private final RecordMapper<K, V> recordRemapper;
    private final Selectors<K, V> fieldsSelectors;
    private final ReentrantLock consumerLock = new ReentrantLock();
    private volatile ConsumerWrapper consumer;
    private AtomicBoolean infoLock = new AtomicBoolean(false);

    private volatile InfoItem infoItem;

    private final ExecutorService pool;

    public ConsumerLoop(
            ConsumerLoopConfig<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener) {
        super(config, metadataListener);
        this.fieldsSelectors = config.fieldMappings().selectors();

        this.recordRemapper =
                RecordMapper.<K, V>builder()
                        .withSelectors(config.itemTemplates().selectors())
                        .withSelectors(fieldsSelectors)
                        .build();

        this.eventListener = eventListener;
        this.pool = Executors.newFixedThreadPool(2);
    }

    @Override
    void startConsuming() throws SubscriptionException {
        log.atDebug().log("START CONSUMER: Acquiring consumer lock...");
        consumerLock.lock();
        log.atDebug().log("START CONSUMER: Lock acquired...");
        try {
            consumer =
                    new ConsumerWrapper(
                            ValueExceptionStrategy.valueOf(config.recordErrorHandlingStrategy()));
            CompletableFuture.runAsync(consumer, pool);
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
            metadataListener.forceUnsubscriptionAll();
        } finally {
            log.atDebug().log("START CONSUMER: Releasing consumer lock...");
            consumerLock.unlock();
            log.atDebug().log("START CONSUMER: Released consumer");
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
            log.atTrace().log("Releasing consumer lock to stop consuming...");
            consumerLock.unlock();
        }
    }

    private class ConsumerWrapper implements Runnable {

        private static AtomicInteger COUNTER = new AtomicInteger(0);

        private final KafkaConsumer<K, V> consumer;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Logger log = LoggerFactory.getLogger(ConsumerWrapper.class);
        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        private final int counter;
        private final Thread hook;

        private ValueExceptionStrategy st;
        private volatile boolean enableFinalCommit = true;

        ConsumerWrapper(ValueExceptionStrategy st) throws KafkaException {
            this.st = st;
            this.counter = COUNTER.incrementAndGet();
            String bootStrapServers =
                    config.consumerProperties()
                            .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            log.atInfo().log("Starting connection to Kafka at {}", bootStrapServers);
            consumer =
                    new KafkaConsumer<>(
                            config.consumerProperties(),
                            config.keyDeserializer(),
                            config.valueDeserializer());
            log.atInfo().log("Established connection to Kafka broker at {}", bootStrapServers);

            this.hook = setShutdownHook();
            log.atDebug().log("Shutdown Hook set");
        }

        private Thread setShutdownHook() {
            Runnable shutdownTask =
                    () -> {
                        log.atInfo().log("Hook shutdown for consumer %d".formatted(counter));
                        shutdown();
                    };

            Thread hook = new Thread(shutdownTask);
            Runtime.getRuntime().addShutdownHook(hook);
            return hook;
        }

        @Override
        public void run() {
            List<String> topics = config.itemTemplates().topics().toList();
            log.atDebug().log("Subscring to topics [{}]", topics);
            consumer.subscribe(
                    topics,
                    new ConsumerRebalanceListener() {

                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            log.atWarn()
                                    .log("Partions revoked, committing offsets {}", currentOffsets);
                            try {
                                consumer.commitSync(currentOffsets);
                            } catch (Exception e) {
                                log.atError()
                                        .setCause(e)
                                        .log(
                                                "An error occured while committing current offsets during after partitions have been revoked");
                            }
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            log.atDebug().log("Assigned partiions {}", partitions);
                        }
                    });
            log.atInfo().log("Subscribed to topics [{}]", topics);
            try {
                while (true) {
                    log.atDebug().log("Polling records");
                    try {
                        ConsumerRecords<K, V> records =
                                consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                        log.atDebug().log("Received records");
                        records.forEach(this::consume);
                        consumer.commitAsync();

                    } catch (ValueException ve) {
                        log.atWarn()
                                .log(
                                        "An error occured while extracting values from the record: {}",
                                        ve.getMessage());
                        notifyException(ve);

                        switch (st) {
                            case IGNORE_AND_CONTINUE -> {
                                log.atWarn().log("Commiting anyway");
                                consumer.commitAsync();
                            }

                            case FORCE_UNSUBSCRIPTION -> {
                                log.atWarn().log("Forcing unsubscription");
                                enableFinalCommit = false;
                                metadataListener.forceUnsubscriptionAll();
                            }
                        }
                    } catch (WakeupException we) {
                        // Catch and rethrow the Exception here because of the next KafkaException
                        throw we;
                    } catch (KafkaException ke) {
                        log.atError().setCause(ke).log("Unrecoverable Kafka exception");
                        metadataListener.forceUnsubscriptionAll();
                    }
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                try {
                    if (enableFinalCommit) {
                        consumer.commitSync();
                    }
                } catch (CommitFailedException e) {
                    log.atWarn().setCause(e).log();
                } finally {
                    consumer.close();
                    log.atDebug().log("Closed Kafka Consumer");
                    latch.countDown();
                    log.atDebug().log("Kafka Consumer closed");
                }
            }
        }

        protected void consume(ConsumerRecord<K, V> record) {
            MappedRecord remappedRecord = recordRemapper.map(record);
            log.atDebug().log("Mapped record to {}", remappedRecord);
            config.itemTemplates()
                    .expand(remappedRecord)
                    .forEach(expandedItem -> processItem(remappedRecord, expandedItem));
            currentOffsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1, null));
        }

        private void processItem(MappedRecord record, Item expandedItem) {
            log.atDebug().log(
                    "Processing expanded item {} against {} subscribed items",
                    expandedItem,
                    subscribedItems.size());
            for (Item subscribedItem : subscribedItems.values()) {
                if (!expandedItem.matches(subscribedItem)) {
                    log.warn(
                            "Expanded item <{}> does not match subscribed item <{}>",
                            expandedItem,
                            subscribedItem);
                    continue;
                }
                log.atDebug().log("Filtering updates");
                Map<String, String> updates = record.filter(fieldsSelectors);

                log.atDebug().log("Sending updates: {}", updates);
                eventListener.smartUpdate(subscribedItem.itemHandle(), updates, false);
            }
        }

        private void closeAndExit(Throwable cause) {
            enableFinalCommit = false;
            close();
            eventListener.failure(cause);
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
                log.atDebug().log("Waiting for graceful thread completion");
                latch.await();
                log.atDebug().log("Thread completed");
            } catch (InterruptedException e) {
                // Ignore
            }
            log.atInfo().log("Kafka consumer shut down");
        }
    }

    @Override
    public void subscribeInfoItem(InfoItem itemHandle) {
        this.infoItem = itemHandle;
    }

    @Override
    public void unsubscribeInfoItem() {
        while (!infoLock.compareAndSet(false, true)) {
            this.infoItemhande = null;
        }
    }

    private void notifyException(Throwable re) {
        infoLock.set(true);
        try {
            if (infoItem == null) {
                return;
            }
            eventListener.smartUpdate(
                    infoItem.itemHandle(), infoItem.mkEvent(re.getMessage()), false);
        } finally {
            infoLock.set(false);
        }
    }
}
