
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

package com.lightstreamer.kafka_connector.adapters.consumers;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapters.ConsumerLoopConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapters.commons.MetadataListener;
import com.lightstreamer.kafka_connector.adapters.config.InfoItem;
import com.lightstreamer.kafka_connector.adapters.config.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueException;

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
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    private final MetadataListener metadataListener;
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
        super(config);
        this.metadataListener = metadataListener;
        this.fieldsSelectors = config.fieldSelectors();
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
        log.atTrace().log("Acquiring consumer lock...");
        consumerLock.lock();
        log.atDebug().log("Lock acquired...");
        try {
            consumer = new ConsumerWrapper(config.recordErrorHandlingStrategy());
            CompletableFuture.runAsync(consumer, pool);
        } catch (KafkaException ke) {
            log.atError().setCause(ke).log("Unable to start consuming from the Kafka brokers");
            metadataListener.forceUnsubscriptionAll();
        } finally {
            log.atTrace().log("Releasing consumer lock...");
            consumerLock.unlock();
            log.atTrace().log("Released consumer");
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

        private final KafkaConsumer<K, V> consumer;
        private final ConsumerLoop<K, V>.RebalancerListener relabancerListener;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Thread hook;
        private final RecordErrorHandlingStrategy errorStrategy;
        private volatile boolean enableFinalCommit = true;

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

            this.relabancerListener = new RebalancerListener(consumer);
            this.hook = setShutdownHook();
            log.atDebug().log("Shutdown Hook set");
        }

        private Thread setShutdownHook() {
            Runnable shutdownTask =
                    () -> {
                        log.atInfo().log("Invoked shutdown Hook");
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
            consumer.subscribe(topics, relabancerListener);
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
                                        "An error occured while extracting values from the record:"
                                                + " {}",
                                        ve.getMessage());
                        log.atWarn().log("Applying the {} strategy", errorStrategy);
                        notifyException(ve);

                        switch (errorStrategy) {
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
            relabancerListener.updateOffsets(record);
        }

        private void processItem(MappedRecord record, Item expandedItem) {
            log.atDebug().log(
                    "Processing expanded item [{}] against [{}] subscribed items",
                    expandedItem,
                    subscribedItems.size());
            for (Item subscribedItem : subscribedItems.values()) {
                if (!expandedItem.matches(subscribedItem)) {
                    log.warn(
                            "Expanded item [{}] does not match subscribed item [{}]",
                            expandedItem,
                            subscribedItem);
                    continue;
                }
                log.atDebug().log("Filtering updates");
                try {
                    Map<String, String> updates = record.filter(fieldsSelectors);
                    log.atDebug().log("Sending updates: {}", updates);
                    eventListener.smartUpdate(subscribedItem.itemHandle(), updates, false);
                } catch (RuntimeException e) {
                    log.atWarn().setCause(e).log();
                    throw e;
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
                log.atTrace().log("Thread completed");
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

    class RebalancerListener implements ConsumerRebalanceListener {

        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        private final KafkaConsumer<?, ?> consumer;

        RebalancerListener(KafkaConsumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.atWarn().log("Partions revoked, committing offsets {}", currentOffsets);
            try {
                consumer.commitSync(currentOffsets);
            } catch (KafkaException e) {
                log.atError()
                        .setCause(e)
                        .log(
                                "An error occured while committing current offsets during after"
                                        + " partitions have been revoked");
                throw e;
            }
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
