package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

public class ConsumerLoop<K, V> extends AbstractConsumerLoop<K, V> {

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private final ItemEventListener eventListener;
    private final ConcurrentHashMap<String, Item> subscribedItems = new ConcurrentHashMap<>();
    private final RecordMapper<K, V> recordRemapper;
    private final Selectors<K, V> fieldsSelectors;
    private final Consumer consumer;
    private final AtomicInteger itemsCounter = new AtomicInteger(0);

    public ConsumerLoop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        super(config);
        this.fieldsSelectors = config.fieldMappings().selectors();

        this.recordRemapper = RecordMapper.<K, V>builder()
                .withSelectors(config.itemTemplates().selectors())
                .withSelectors(fieldsSelectors)
                .build();

        this.eventListener = eventListener;
        this.consumer = new Consumer();
        Executors.newSingleThreadExecutor().submit(consumer);
    }

    private class Consumer implements Runnable {

        private volatile KafkaConsumer<K, V> consumer;
        private final CountDownLatch latch = new CountDownLatch(1);
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
                    if (itemsCounter.get() > 0) {
                        log.atDebug().log("Received records");
                        records.forEach(this::consume);
                    } else {
                        log.atDebug().log("Record received but skipped as there is no subscribed item");
                    }
                }
            } catch (WakeupException e) {
                log.atDebug().log("Kafka Consumer woken up");
            } finally {
                log.atDebug().log("Start closing Kafka Consumer");
                consumer.close();
                latch.countDown();
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
                log.atDebug().log("Filtering updates");
                Map<String, String> updates = record.filter(fieldsSelectors);

                log.atDebug().log("Sending updates: {}", updates);
                eventListener.smartUpdate(subscribedItem.itemHandle(), updates, false);
            }
        }

        void shutdown() throws InterruptedException {
            log.atInfo().log("Shutting down Kafka consumer");
            consumer.wakeup();
            latch.await();
            log.atInfo().log("Kafka consumer shut down");
        }
    }
}
