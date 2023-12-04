package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka_connector.adapter.Loop;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.Item;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemTemplate;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordInspector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelectorSupplier;

public class ConsumerLoop<K, V> implements Loop {

    private final Properties properties = new Properties();

    private final ItemEventListener eventListener;

    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private final List<TopicMapping> topicMappings;

    private final Map<String, Item> subscribedItems = new ConcurrentHashMap<>();

    private final List<ItemTemplate<K, V>> itemTemplates;

    private final CyclicBarrier barrier;

    private final RecordInspector<K, V> recordInspector;

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    public ConsumerLoop(Map<String, String> configuration,
            List<TopicMapping> topicMappings,
            KeySelectorSupplier<K> ks,
            ValueSelectorSupplier<V> vs,
            ItemEventListener eventListener) {

        this.eventListener = eventListener;
        this.topicMappings = topicMappings;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("adapter.dir", configuration.get("adapter.dir"));
        Optional.ofNullable(configuration.get("key.schema.file"))
                .ifPresent(v -> properties.setProperty("key.schema.file", v));
        Optional.ofNullable(configuration.get("value.schema.file"))
                .ifPresent(v -> properties.setProperty("value.schema.file", v));
        log.info("Properties: {}", properties);
        ks.configKey(configuration, properties);
        vs.configValue(configuration, properties);

        log.info("Creating item templates");
        itemTemplates = ItemTemplate.fromTopicMappings(topicMappings, RecordInspector.builder(ks, vs));
        recordInspector = instruct(configuration, RecordInspector.builder(ks, vs));

        barrier = new CyclicBarrier(2);
    }

    RecordInspector<K, V> instruct(Map<String, String> configuration, RecordInspector.Builder<K, V> builder) {
        configuration.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("field."))
                .map(e -> new Pair<>(
                        e.getKey().substring(e.getKey().indexOf(".") + 1),
                        e.getValue()))
                .forEach(p -> builder.instruct(p.first(), p.second()));

        return builder.build();
    }

    @Override
    public void trySubscribe(String item, Object itemHandle) {
        Item subscribedItem = subscribedItems.computeIfAbsent(item, it -> {
            for (ItemTemplate<K, V> itemTemplate : this.itemTemplates) {
                Item newItem = Item.of(it, itemHandle);
                MatchResult result = itemTemplate.match(newItem);
                if (result.matched()) {
                    log.info("Subscribed to {}", it);
                    return newItem;
                }
            }
            return null;
        });
        if (subscribedItem != null) {
            start();
        }
    }

    void start() {
        if (!(isSubscribed.compareAndSet(false, true))) {
            return;
        }
        // Create consumer
        log.info("Connecting to Kafka at {}", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        barrier.reset();

        try {
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties);
            log.info("Connected to Kafka");

            List<String> topics = topicMappings.stream().map(TopicMapping::topic).toList();
            consumer.subscribe(topics);
            CompletableFuture.runAsync(() -> {
                // Poll for new data
                try {
                    while (isSubscribed.get()) {
                        log.debug("Polling from topics {} ...", topics);
                        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
                        records.forEach(this::consume);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    consumer.close();
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    log.info("Disconnected from Kafka");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void consume(ConsumerRecord<K, V> record) {
        itemTemplates.stream()
                .filter(template -> template.topic().equals(record.topic()))
                .map(template -> template.expand(record))
                .forEach(expandedItem -> processItem(record, expandedItem));
    }

    private void processItem(ConsumerRecord<K, V> record, Item expandedItem) {
        for (Item subscribedItem : subscribedItems.values()) {
            if (!expandedItem.match(subscribedItem).matched()) {
                log.warn("Expanded item <{}> does not match subscribed item <{}>",
                        expandedItem,
                        subscribedItem);
                continue;
            }
            // log.info("Sending updates");
            List<Value> updates = recordInspector.inspect(record);
            Map<String, String> values = updates.stream().collect(Collectors.toMap(Value::name, Value::text));
            eventListener.smartUpdate(subscribedItem.getItemHandle(), values, false);
        }
    }

    @Override
    public void unsubscribe(String item) {
        subscribedItems.remove(item);
        if (subscribedItems.size() == 0) {
            stop();
        }
    }

    void stop() {
        if (isSubscribed.compareAndSet(true, false)) {
            try {
                barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
}
