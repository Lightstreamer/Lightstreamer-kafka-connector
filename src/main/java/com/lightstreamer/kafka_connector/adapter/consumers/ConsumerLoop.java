package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka_connector.adapter.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueException;

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private final ItemEventListener eventListener;
    private final RecordMapper<K, V> recordRemapper;
    private final Selectors<K, V> fieldsSelectors;
    private final ReentrantLock consumerLock = new ReentrantLock();
    private volatile ConsumerWrapper consumer;

    public ConsumerLoop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        super(config);
        this.fieldsSelectors = config.fieldMappings().selectors();

        this.recordRemapper = RecordMapper.<K, V>builder()
                .withSelectors(config.itemTemplates().selectors())
                .withSelectors(fieldsSelectors)
                .build();

        this.eventListener = eventListener;
    }

    @Override
    void startConsuming() {
        consumerLock.lock();
        try {
            consumer = new ConsumerWrapper();
            new Thread(consumer).start();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    void stopConsuming() {
        consumerLock.lock();
        try {
            if (consumer != null) {
                consumer.close();
            }
        } finally {
            consumerLock.unlock();
        }

    }

    private class ConsumerWrapper implements Runnable {

        private static AtomicInteger COUNTER = new AtomicInteger(0);

        private final KafkaConsumer<K, V> consumer;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Logger log = LoggerFactory.getLogger(ConsumerWrapper.class);
        private int counter;
        private Thread hook;

        ConsumerWrapper() {
            this.counter = COUNTER.incrementAndGet();
            String bootStrapServers = config.consumerProperties()
                    .getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            log.atInfo().log("Starting connection to Kafka at {}",
                    bootStrapServers);
            consumer = new KafkaConsumer<>(config.consumerProperties(), config.keyDeserializer(),
                    config.valueDeserializer());
            log.atInfo().log("Established connection to Kafka broker at {}", bootStrapServers);

            setShutdownHook();
            log.atDebug().log("Shutdown Hook set");
        }

        private void setShutdownHook() {
            Runnable shutdownTask = () -> {
                log.atInfo().log("Hook shutdown for consumer %d".formatted(counter));
                shutdown();
            };

            this.hook = new Thread(shutdownTask);
            Runtime.getRuntime().addShutdownHook(hook);
        }

        @Override
        public void run() {
            try {
                List<String> topics = config.itemTemplates().topics().toList();
                log.atDebug().log("Subscring to {}", topics);
                consumer.subscribe(topics);
                log.atInfo().log("Subscribed to {}", topics);

                while (true) {
                    log.atDebug().log("Polling records");
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    log.atDebug().log("Received records");
                    try {
                        records.forEach(this::consume);
                        consumer.commitAsync();
                    } catch (ValueException re) {
                        log.atInfo().setCause(re).log();
                    }
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    log.atWarn().setCause(e).log();
                } finally {
                    consumer.close();
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
        }

        private void processItem(MappedRecord record, Item expandedItem) {
            log.atDebug().log("Processing expanded item {} against {} subscribed items", expandedItem,
                    subscribedItems.size());
            for (Item subscribedItem : subscribedItems.values()) {
                if (!expandedItem.matches(subscribedItem)) {
                    log.warn("Expanded item <{}> does not match subscribed item <{}>", expandedItem, subscribedItem);
                    continue;
                }
                log.atDebug().log("Filtering updates");
                Map<String, String> updates = record.filter(fieldsSelectors);

                log.atDebug().log("Sending updates: {}", updates);
                eventListener.smartUpdate(subscribedItem.itemHandle(), updates, false);
            }
        }

        void close() {
            shutdown();
            Runtime.getRuntime().removeShutdownHook(this.hook);
        }

        private void shutdown() {
            log.atInfo().log("Shutting down Kafka consumer");
            consumer.wakeup();
            try {
                latch.await();
            } catch (InterruptedException e) {
                // Ignore
            }
            log.atInfo().log("Kafka consumer shut down");
        }
    }
}
