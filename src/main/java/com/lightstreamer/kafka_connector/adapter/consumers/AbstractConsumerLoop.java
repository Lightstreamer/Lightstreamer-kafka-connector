package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
import com.lightstreamer.kafka_connector.adapter.KafkaConnectorAdapter;
import com.lightstreamer.kafka_connector.adapter.Loop;
import com.lightstreamer.kafka_connector.adapter.evaluator.EvaluatorFactory;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.StatementEvaluator;

public abstract class AbstractConsumerLoop<T> implements Loop {

    public static record TopicMapping(String topic, String[] itemTemplates) {
    }

    protected final Properties properties = new Properties();

    private Map<String, RecordEvaluator<T>> recordEvaluators;

    protected ItemEventListener eventListener;

    protected AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private EvaluatorFactory<T> evaluatorFactory;

    private TopicMapping topicMapping;

    private Set<String> subscribedItems = ConcurrentHashMap.newKeySet();

    private List<StatementEvaluator<T>> itemEvaluators;

    private CyclicBarrier barrier;

    protected static Logger log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);

    AbstractConsumerLoop(Map<String, String> configuration, TopicMapping topicMapping, EvaluatorFactory<T> ef,
            ItemEventListener eventListener) {
        this.eventListener = eventListener;
        this.evaluatorFactory = ef;
        this.topicMapping = topicMapping;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        recordEvaluators = configuration.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("item_"))
                .map(e -> new Pair<>(
                        e.getKey().substring(e.getKey().indexOf("_") + 1),
                        e.getValue()))
                .collect(Collectors.toMap(Pair::first, p -> ef.get(p.second())));

        itemEvaluators = Arrays.stream(topicMapping.itemTemplates())
                .map(itemTemplate -> new StatementEvaluator<>(itemTemplate, ef)).toList();

        barrier = new CyclicBarrier(2);
    }

    public EvaluatorFactory<T> getEvaluatorFactory() {
        return evaluatorFactory;
    }

    @Override
    public boolean maySubscribe(String item) {
        return itemEvaluators.stream().anyMatch(s -> s.match(item));
    }

    @Override
    public void subscribe(String item) {
        subscribedItems.add(item);
        if (isSubscribed.compareAndSet(false, true)) {
            start();
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
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        // TODO Auto-generated catch block
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
        itemEvaluators.stream().forEach(itemEvaluator -> {
            String expandedItem = itemEvaluator.replace(record.value());
            for (String subscribedItem : subscribedItems) {
                if (!subscribedItem.equals(expandedItem)) {
                    log.warn("Expanded item <{}> does not match subscribed item <{}>", expandedItem, subscribedItem);
                }
                Map<String, String> updates = new HashMap<>();
                for (Map.Entry<String, RecordEvaluator<T>> entry : recordEvaluators.entrySet()) {
                    RecordEvaluator<T> re = entry.getValue();
                    String value = re.extract(record.value());
                    log.info("Extracted <{}> -> <{}>", value, entry.getKey());
                    updates.put(entry.getKey(), value);
                }
                log.info("Sending updates {}", updates);
                eventListener.update(subscribedItem, updates, false);
            }
        });
    }

}
