package com.lightstreamer.kafka_connector.adapter.consumers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka_connector.adapter.Loop;
import com.lightstreamer.kafka_connector.adapter.config.ConfigParser.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector.RemappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors;

public class ConsumerLoop<K, V> implements Loop {

	private final Properties properties;

	private final ItemEventListener eventListener;

	private final AtomicBoolean isSubscribed = new AtomicBoolean(false);

	private final Map<String, Item> subscribedItems = new ConcurrentHashMap<>();

	private final CyclicBarrier barrier;

	private final RecordInspector<K, V> recordInspector;

	private final Selectors<K, V> fieldsSelectors;

	private final ItemTemplates<K, V> itemTemplates;

	protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

	public ConsumerLoop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
		this.properties = config.consumerProperties();
		this.itemTemplates = config.itemTemplates();
		this.fieldsSelectors = config.fieldsSelectors();

		recordInspector = RecordInspector.<K, V>builder()
				.withItemTemplates(itemTemplates)
				.withSelectors(fieldsSelectors)
				.build();

		this.eventListener = eventListener;

		barrier = new CyclicBarrier(2);
	}

	@Override
	public void trySubscribe(String item, Object itemHandle) {
		Item subscribedItem = subscribedItems.computeIfAbsent(item, it -> {
			try {
				Item newItem = Items.itemFrom(it, itemHandle);
				if (itemTemplates.matches(newItem)) {
					log.info("Subscribed to {}", it);
					return newItem;
				}
			} catch (Exception e) {
				e.printStackTrace();
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

			List<String> topics = itemTemplates.topics().toList();
			consumer.subscribe(topics);
			CompletableFuture.runAsync(() -> {
				// Poll for new data
				try {
					while (isSubscribed.get()) {
						log.debug("Polling from topics {} ...", topics);
						try {
							ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
							records.forEach(this::consume);
						} catch (Exception e) {
							e.printStackTrace();
						}
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
		RemappedRecord remappedRecord = recordInspector.extract(record);
		itemTemplates.expand(remappedRecord).forEach(expandedItem -> processItem(remappedRecord, expandedItem));
	}

	private void processItem(RemappedRecord record, Item expandedItem) {
		for (Item subscribedItem : subscribedItems.values()) {
			if (!expandedItem.matches(subscribedItem)) {
				log.warn("Expanded item <{}> does not match subscribed item <{}>", expandedItem, subscribedItem);
				continue;
			}
			// log.info("Sending updates");
			eventListener.smartUpdate(subscribedItem.itemHandle(), record.filter(fieldsSelectors.schema()), false);
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
