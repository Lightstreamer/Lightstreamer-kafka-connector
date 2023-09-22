package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SmartDataProvider;
import com.lightstreamer.interfaces.data.SubscriptionException;

public class KafkaConnectorAdapter implements SmartDataProvider {

    private Logger log;

    private ItemEventListener eventListener;

    private volatile boolean isSubscribed = false;

    private String topic;

    private Map<String, String> configuration;

    static ConfigSpec CONFIG_SPEC;
    static {
        CONFIG_SPEC = new ConfigSpec()
                .add("bootstrap-servers", true, new ListType<ConfType>(ConfType.Host))
                .add("topic", true, ConfType.Text)
                .add("group-id", true, ConfType.Text);
    }

    public KafkaConnectorAdapter() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map params, File configDir) throws DataProviderException {
        Path path = Paths.get(configDir.getAbsolutePath(), "log4j.properties");
        PropertyConfigurator.configure(path.toString());
        log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);
        try {
            this.configuration = CONFIG_SPEC.parse(params);
            this.topic = configuration.get("topic");
        } catch (ValidateException ve) {
            throw new DataProviderException(ve.getMessage());
        }
    }

    @Override
    public void unsubscribe(String itemName) throws SubscriptionException, FailureException {
        if (itemName.equals(topic)) {
            isSubscribed = false;
            log.info("Unsubscribed from item [{}] item%n", itemName);
        }
    }

    @Override
    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public void setListener(ItemEventListener eventListener) {
        this.eventListener = eventListener;

    }

    public void subscribe(String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {
    }

    @Override
    public void subscribe(String itemName, Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        connect(itemName, itemHandle);

    }

    private void connect(String itemName, Object itemHandle) {
        // if (!itemName.equals(topic)) {
        //     return;
        // }

        isSubscribed = true;

        String bootstrapServers = this.configuration.get("bootstrap-servers");
        String groupId = this.configuration.get("group-id");

        CompletableFuture.runAsync(() -> {
            Thread.currentThread().setContextClassLoader(null);
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            log.info("Connecting to Kafka at {}", bootstrapServers);
            try {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
                log.info("Connected to Kafka");

                // subscribe consumer to our topic(s)
                log.info("Subscribing to topic \"{}\"", topic);
                consumer.subscribe(Arrays.asList(topic));
                log.info("Subscribed to topic \"{}\"", topic);

                // poll for new data
                while (isSubscribed) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        // log.info("Key: " + record.key() + ", Value: " + record.value());
                        // log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                        Map<String, String> updates = new HashMap<>();
                        updates.put("key", record.key());
                        updates.put("value", record.value());
                        updates.put("partition", String.valueOf(record.partition()));
                        log.info("Received message: {}", record.value());
                        eventListener.smartUpdate(itemHandle, updates, false);
                    }
                }
                consumer.close();
                log.info("Disconnected from Kafka");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
