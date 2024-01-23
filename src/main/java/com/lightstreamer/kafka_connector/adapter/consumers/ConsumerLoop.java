package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.Loop;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;

public class ConsumerLoop<K, V> implements Loop {

    private final ConsumerLoopConfig<K, V> config;
    private final ItemEventListener eventListener;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private final Map<String, Item> subscribedItems = new ConcurrentHashMap<>();
    private final RecordMapper<K, V> recordRemapper;
    private final Selectors<K, V> fieldsSelectors;

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private volatile Consumer currentConsumer;

    public ConsumerLoop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        this.fieldsSelectors = config.fieldMappings().selectors();
        this.config = config;

        recordRemapper = RecordMapper.<K, V>builder()
                .withSelectors(config.itemTemplates().selectors())
                .withSelectors(fieldsSelectors)
                .build();

        this.eventListener = eventListener;
    }

    @Override
    public void trySubscribe(String item, Object itemHandle) throws SubscriptionException {
        Item subscribedItem = subscribedItems.computeIfAbsent(item, it -> {
            try {
                Item newItem = Items.itemFrom(it, itemHandle);
                if (config.itemTemplates().matches(newItem)) {
                    log.info("Subscribed to {}", it);
                    return newItem;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        });
        if (subscribedItem != null) {
            if (!(isSubscribed.compareAndSet(false, true))) {
                return;
            }
            try {
                if (currentConsumer != null) {
                    throw new RuntimeException("Unexpected consumer");
                }
                currentConsumer = new Consumer();
                CompletableFuture.runAsync(currentConsumer);
            } catch (Exception e) {
                log.atWarn().setCause(e).log();
                throw new SubscriptionException("An error occured while creating a new Kafka consumer");
            }
        }
    }

    @Override
    public void unsubscribe(String item) {
        subscribedItems.remove(item);
        if (subscribedItems.size() != 0) {
            return;
        }
        if (isSubscribed.compareAndSet(true, false)) {
            try {
                currentConsumer.shutdown();
                currentConsumer = null;
            } catch (InterruptedException e) {
                log.atWarn().setCause(e).log();
            }
        }
    }

    private class Consumer implements Runnable {

        private final KafkaConsumer<K, V> consumer;
        private final CountDownLatch shutdownLatch = new CountDownLatch(1);
        private final Logger log = LoggerFactory.getLogger(Consumer.class);

        Consumer() {
            log.atInfo().log("Connecting to Kafka at {}",
                    config.consumerProperties().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            consumer = new KafkaConsumer<>(config.consumerProperties(), config.keyDeserializer(),
                    config.valueDeserializer());
            log.atInfo().log("Connected to Kafka broker");
            setShutdownHook();
            log.atDebug().log("Shutdown Hook set");
        }

        private void setShutdownHook() {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        Consumer.this.shutdown();
                    } catch (InterruptedException e) {
                    }
                }
            });
        }

        @Override
        public void run() {
            try {
                List<String> topics = config.itemTemplates().topics().toList();
                log.atDebug().log("Subscring to {}", topics);
                consumer.subscribe(topics);
                log.atDebug().log("Subscribed to {}", topics);

                while (true) {
                    log.atDebug().log("Polling records");
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    log.atDebug().log("Record receiveds");
                    records.forEach(this::consume);
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                consumer.close();
                shutdownLatch.countDown();
                log.atDebug().log("Kafka Consumer closed");
            }
        }

        protected void consume(ConsumerRecord<K, V> record) {
            MappedRecord remappedRecord = recordRemapper.map(record);
            config.itemTemplates()
                    .expand(remappedRecord)
                    .forEach(expandedItem -> processItem(remappedRecord, expandedItem));
        }

        private void processItem(MappedRecord record, Item expandedItem) {
            for (Item subscribedItem : subscribedItems.values()) {
                if (!expandedItem.matches(subscribedItem)) {
                    log.warn("Expanded item <{}> does not match subscribed item <{}>", expandedItem, subscribedItem);
                    continue;
                }
                log.info("Sending updates");
                eventListener.smartUpdate(subscribedItem.itemHandle(), record.filter(fieldsSelectors), false);
            }
        }

        void shutdown() throws InterruptedException {
            log.atInfo().log("Shutting down Kafka consumer");
            consumer.wakeup();
            shutdownLatch.await();
            log.atInfo().log("Kafka consumer down");
        }
    }
}
