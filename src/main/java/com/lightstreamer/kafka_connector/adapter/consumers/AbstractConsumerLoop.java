package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
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
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;

public abstract class AbstractConsumerLoop<T> implements Loop {

    public static record TopicMapping(String topic, String[] itemTemplates) {
    }

    protected final Properties properties = new Properties();

    private List<ValueSelector<T>> valueSelectors;

    protected ItemEventListener eventListener;

    protected AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private BiFunction<String, String, ValueSelector<T>> evaluatorFactory;

    private TopicMapping topicMapping;

    private Set<Item> subscribedItems = ConcurrentHashMap.newKeySet();

    private List<ItemTemplate<T>> itemTemplates;

    private CyclicBarrier barrier;

    protected static Logger log = LoggerFactory.getLogger(AbstractConsumerLoop.class);

    AbstractConsumerLoop(Map<String, String> configuration, TopicMapping topicMapping,
            BiFunction<String, String, ValueSelector<T>> ef,
            ItemEventListener eventListener) {
        this.eventListener = eventListener;
        this.evaluatorFactory = ef;
        this.topicMapping = topicMapping;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("Creating field evaluators");
        valueSelectors = configuration.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("item_"))
                .map(e -> new Pair<>(
                        e.getKey().substring(e.getKey().indexOf("_") + 1),
                        e.getValue()))
                .map(p -> ef.apply(p.first(), p.second()))
                .toList();

        log.info("Creating item templates for topic <{}>", topicMapping.topic());
        itemTemplates = Arrays
                .stream(topicMapping.itemTemplates())
                .map(s -> ItemTemplate.makeNew(s, ef))
                .toList();
        log.info("Item evaluator created");

        barrier = new CyclicBarrier(2);
    }

    @Override
    public boolean maySubscribe(String item) {
        boolean anyMatch = itemTemplates.stream().anyMatch(s -> {
            Item fromItem = Item.fromItem(item);
            MatchResult match = s.match(fromItem);
            log.info("Item <{}> matches <{}>: {}", s, fromItem, match.matched());
            return match.matched();
        });
        return anyMatch;
    }

    @Override
    public void subscribe(String item) {
        Item fromItem = Item.fromItem(item);
        subscribedItems.add(fromItem);
        log.info("Subscribed to {}", fromItem);
        if (isSubscribed.compareAndSet(false, true)) {
            start();
        }
    }

    @Override
    public void unsubscribe(String item) {
        subscribedItems.remove(Item.fromItem(item));
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

    KafkaConsumer<String, T> newKafkaConsumer(Properties properties) {
        return new KafkaConsumer<>(properties);
    }

    void start() {
        // Create consumer
        log.info("Connecting to Kafka at {}", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        barrier.reset();

        try {
            KafkaConsumer<String, T> consumer = newKafkaConsumer(properties);
            log.info("Connected to Kafka");

            consumer.subscribe(List.of(topicMapping.topic));
            CompletableFuture.runAsync(() -> {
                // Poll for new data
                try {
                    while (isSubscribed.get()) {
                        log.debug("Polling from topic {} ...", topicMapping.topic());
                        ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
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

    protected void consume(ConsumerRecord<String, T> record) {
        itemTemplates.stream().forEach(itemTemplate -> {
            Item expandedItem = itemTemplate.expand(record.value());
            for (Item subscribedItem : subscribedItems) {
                if (expandedItem.match(subscribedItem).matched()) {
                    log.warn("Expanded item <{}> does not match subscribed item <{}>", expandedItem, subscribedItem);
                    continue;
                }

                Map<String, String> updates = new HashMap<>();
                for (ValueSelector<T> valueSelector : valueSelectors) {
                    Value value = valueSelector.extract(record.value());
                    log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
                    updates.put(value.name(), value.text());
                }
                log.info("Sending updates [{}] from topic <{}> to item <{}>", updates, topicMapping.topic(),
                        subscribedItem);
                eventListener.update(subscribedItem.getSourceItem(), updates, false);
            }
        });
    }

}
