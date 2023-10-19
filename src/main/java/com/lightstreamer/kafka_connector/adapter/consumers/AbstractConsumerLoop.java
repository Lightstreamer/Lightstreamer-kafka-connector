package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

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

    protected final Properties properties = new Properties();

    private List<ValueSelector<String, T>> valueSelectors;

    protected ItemEventListener eventListener;

    protected AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private List<TopicMapping> topicMappings;

    private Map<String, Item> subscribedItems = new ConcurrentHashMap<>();

    private List<ItemTemplate<T>> itemTemplates = new ArrayList<>();

    private CyclicBarrier barrier;

    protected static Logger log = LoggerFactory.getLogger(AbstractConsumerLoop.class);

    AbstractConsumerLoop(Map<String, String> configuration,
            List<TopicMapping> topicMappings,
            BiFunction<String, String, ValueSelector<String, T>> ef,
            ItemEventListener eventListener) {
        this.eventListener = eventListener;
        this.topicMappings = topicMappings;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("Creating field evaluators");
        valueSelectors = createValueSelector(configuration, ef);

        log.info("Creating item templates");
        itemTemplates = createItemTemplates(topicMappings, ef);

        barrier = new CyclicBarrier(2);
    }

    private List<ItemTemplate<T>> createItemTemplates(
            List<TopicMapping> topicMappings,
            BiFunction<String, String, ValueSelector<String, T>> ef) {

        return topicMappings.stream().flatMap(t -> t.createItemTemplates(ef).stream()).toList();
    }

    private List<ValueSelector<String, T>> createValueSelector(
            Map<String, String> configuration,
            BiFunction<String, String, ValueSelector<String, T>> ef) {
        return configuration.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("field."))
                .map(e -> new Pair<>(
                        e.getKey().substring(e.getKey().indexOf(".") + 1),
                        e.getValue()))
                .map(p -> ef.apply(p.first(), p.second()))
                .toList();
    }

    @Override
    public void trySubscribe(String item, Object itemHandle) {
        Item subscribedItem = subscribedItems.computeIfAbsent(item, it -> {
            for (ItemTemplate<T> itemTemplate : itemTemplates) {
                Item newItem = Item.fromItem(it, itemHandle);
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
            KafkaConsumer<String, T> consumer = new KafkaConsumer<>(properties);
            log.info("Connected to Kafka");

            List<String> topics = topicMappings.stream().map(TopicMapping::topic).toList();
            consumer.subscribe(topics);
            CompletableFuture.runAsync(() -> {
                // Poll for new data
                try {
                    while (isSubscribed.get()) {
                        log.debug("Polling from topics {} ...", topics);
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
        itemTemplates.stream()
                .filter(template -> template.topic().equals(record.topic()))
                .map(template -> template.expand(record))
                .forEach(expandedItem -> processItem(record, expandedItem));
    }

    private void processItem(ConsumerRecord<String, T> record, Item expandedItem) {
        for (Item subscribedItem : subscribedItems.values()) {
            if (!expandedItem.match(subscribedItem).matched()) {
                log.warn("Expanded item <{}> does not match subscribed item <{}>",
                        expandedItem,
                        subscribedItem);
                continue;
            }
            // log.info("Sending updates");
            Map<String, String> updates = new HashMap<>();
            for (ValueSelector<String, T> valueSelector : valueSelectors) {
                Value value = valueSelector.extract(record);
                log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
                updates.put(value.name(), value.text());
            }
            log.info("Sending updates [{}] from topic <{}> to item <{}>",
                    updates.keySet(),
                    record.topic(),
                    subscribedItem);
            eventListener.smartUpdate(subscribedItem.getItemHandle(), updates, false);
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
