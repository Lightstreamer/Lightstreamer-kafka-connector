package com.lightstreamer.kafka_connector.adapter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.ItemEventListener;

public abstract class AbstractConsumerLoop<T> implements Loop {

    protected final Properties properties = new Properties();

    protected List<String> subscribedTopics = new ArrayList<>();

    protected ItemEventListener eventListener;

    protected volatile boolean isSubscribed = false;

    private CompletableFuture<Void> f;

    protected static Logger log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);

    AbstractConsumerLoop(Map<String, String> configuration, ItemEventListener eventListener) {
        this.eventListener = eventListener;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // properties.put("key.deserializer", StringDeserializer.class.getName());
        // properties.put("value.deserializer", StringDeserializer.class.getName());
    }

    @Override
    public void subscribe(String topic) {
        subscribedTopics.add(topic);
        start();
    }

    @Override
    public void unsubscribe(String topic) {
        stop();
        subscribedTopics.remove(topic);
        f.join();
    }

    void stop() {
        isSubscribed = false;
    }

    abstract KafkaConsumer<String, T> newKafkaConsumer(Properties properties);

    void start() {
        // Create consumer
        isSubscribed = true;
        log.info("Connecting to Kafka at {}", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        try {
            KafkaConsumer<String, T> consumer = newKafkaConsumer(properties);
            log.info("Connected to Kafka");

            consumer.subscribe(new ArrayList<>(subscribedTopics));
            f = CompletableFuture.runAsync(() -> {
                // Poll for new data
                try {
                    while (isSubscribed) {
                        ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
                        // log.debug("Polling data");
                        for (ConsumerRecord<String, T> record : records) {
                            consume(record);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                consumer.close();
                log.info("Disconnected from Kafka");

            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void consume(ConsumerRecord<String, T> record);

}
