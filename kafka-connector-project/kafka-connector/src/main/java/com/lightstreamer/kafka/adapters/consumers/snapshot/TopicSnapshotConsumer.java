
/*
 * Copyright (C) 2025 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.adapters.consumers.snapshot;

import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

/**
 * A shared, per-topic Kafka consumer that delivers snapshot data from a compacted topic to all
 * pending subscribed items.
 *
 * <p>This consumer is ephemeral and short-lived. It reads the full compacted state of a topic from
 * the beginning, routes each record to matching subscribed items, and delivers snapshot events.
 * Multiple items pending snapshot on the same topic are served in a single pass (or a few passes if
 * items arrive while a pass is running).
 *
 * <p>Key design properties:
 *
 * <ul>
 *   <li>Uses {@code assign()} with no consumer group — avoids polluting the main consumer's
 *       offsets.
 *   <li>Reads from topic beginning to end offsets captured at pass start.
 *   <li>Routes records to pending items via {@link RecordMapper} and a scoped {@link
 *       SubscribedItems}.
 *   <li>Late-arriving items are served in subsequent passes.
 *   <li>Closes the Kafka consumer and exits when no more items are pending.
 * </ul>
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 */
public class TopicSnapshotConsumer<K, V> implements Runnable {

    /**
     * Callback interface notified when the consumer has finished all passes and is shutting down.
     */
    public interface CompletionCallback {
        void onSnapshotConsumerCompleted(String topic);
    }

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000);

    private final String topic;
    private final Logger logger;
    private final RecordMapper<K, V> recordMapper;
    private final EventListener eventListener;
    private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;
    private final DeserializerPair<K, V> deserializerPair;
    private final Duration snapshotTimeout;
    private final CompletionCallback completionCallback;

    private final ConcurrentLinkedQueue<SubscribedItem> pendingItems =
            new ConcurrentLinkedQueue<>();

    /** Holds the active consumer reference for wakeup-based shutdown. */
    private volatile Consumer<byte[], byte[]> activeConsumer;

    /**
     * Creates a new {@code TopicSnapshotConsumer} for the given topic.
     *
     * @param topic the Kafka topic to read snapshot data from
     * @param logger the logger instance
     * @param recordMapper the mapper used to extract canonical item names and fields from records
     * @param eventListener the listener for dispatching snapshot events to Lightstreamer
     * @param consumerSupplier supplier for creating a Kafka consumer (no group.id, auto.commit off)
     * @param deserializerPair the {@link DeserializerPair} for record keys and values
     * @param snapshotTimeout maximum duration for a single snapshot pass; {@link Duration#ZERO}
     *     means no limit
     * @param completionCallback callback invoked when this consumer finishes all passes
     */
    public TopicSnapshotConsumer(
            String topic,
            Logger logger,
            RecordMapper<K, V> recordMapper,
            EventListener eventListener,
            Supplier<Consumer<byte[], byte[]>> consumerSupplier,
            DeserializerPair<K, V> deserializerPair,
            Duration snapshotTimeout,
            CompletionCallback completionCallback) {
        this.topic = topic;
        this.logger = logger;
        this.recordMapper = recordMapper;
        this.eventListener = eventListener;
        this.consumerSupplier = consumerSupplier;
        this.deserializerPair = deserializerPair;
        this.snapshotTimeout = snapshotTimeout;
        this.completionCallback = completionCallback;
    }

    /**
     * Enqueues a subscribed item to receive snapshot data. Thread-safe; can be called from any
     * thread while the consumer is running. The item will be included in the current pass (if not
     * yet started) or the next pass.
     *
     * @param item the subscribed item awaiting snapshot delivery
     */
    public void enqueue(SubscribedItem item) {
        logger.atDebug().log("Enqueuing item [{}] for snapshot on topic [{}]", item.name(), topic);
        pendingItems.add(item);
    }

    @Override
    public void run() {
        Consumer<byte[], byte[]> consumer = consumerSupplier.get();
        this.activeConsumer = consumer;
        try {
            List<TopicPartition> partitions = discoverPartitions(consumer);
            if (partitions.isEmpty()) {
                logger.atWarn().log("No partitions found for topic [{}], skipping snapshot", topic);
                drainAndFinalizeAll();
                return;
            }

            consumer.assign(partitions);

            // Run passes until no more pending items
            while (true) {
                Set<SubscribedItem> passItems = drainPendingItems();
                if (passItems.isEmpty()) {
                    break;
                }

                logger.atInfo().log(
                        "Starting snapshot pass for topic [{}] with {} items",
                        topic,
                        passItems.size());

                runPass(consumer, partitions, passItems);

                logger.atInfo().log(
                        "Snapshot pass complete for topic [{}], finalized {} items",
                        topic,
                        passItems.size());
            }
        } catch (WakeupException e) {
            logger.atDebug().log("Snapshot consumer for topic [{}] woken up, shutting down", topic);
            drainAndFinalizeAll();
        } catch (KafkaException e) {
            logger.atError()
                    .setCause(e)
                    .log("Unrecoverable error during snapshot for topic [{}]", topic);
            drainAndFinalizeAll();
        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Unexpected error during snapshot for topic [{}]", topic);
            drainAndFinalizeAll();
        } finally {
            this.activeConsumer = null;
            closeConsumer(consumer);
            completionCallback.onSnapshotConsumerCompleted(topic);
        }
    }

    /**
     * Executes a single snapshot pass: seeks to beginning, reads until end offsets, routes to
     * matching items.
     */
    private void runPass(
            Consumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions,
            Set<SubscribedItem> passItems) {

        // Build a scoped SubscribedItems containing only items in this pass
        SubscribedItems snapshotScope = scopedItems(passItems);

        // Seek to beginning and capture end offsets as snapshot boundary
        consumer.seekToBeginning(partitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        // Track consumed offsets per partition
        Map<TopicPartition, Long> currentOffsets = new HashMap<>();

        long passStartTime = System.currentTimeMillis();
        boolean timedOut = false;

        while (!allPartitionsConsumed(currentOffsets, endOffsets)) {
            // Check timeout
            if (hasTimeoutElapsed(passStartTime)) {
                logger.atWarn()
                        .log(
                                "Snapshot pass for topic [{}] timed out after {} ms",
                                topic,
                                snapshotTimeout.toMillis());
                timedOut = true;
                break;
            }

            ConsumerRecords<byte[], byte[]> records = doPoll(consumer);
            for (ConsumerRecord<byte[], byte[]> raw : records) {
                try {
                    KafkaRecord<K, V> kafkaRecord = deserialize(raw);
                    MappedRecord mapped = recordMapper.map(kafkaRecord);
                    Set<SubscribedItem> routed = mapped.route(snapshotScope);
                    if (!routed.isEmpty()) {
                        Map<String, String> fields = mapped.fieldsMap();
                        for (SubscribedItem item : routed) {
                            item.sendSnapshotEvent(fields, eventListener);
                        }
                    }
                } catch (ValueException e) {
                    logger.atWarn()
                            .setCause(e)
                            .log(
                                    "Skipping record at offset {} on partition {}-{} due to extraction error",
                                    raw.offset(),
                                    raw.topic(),
                                    raw.partition());
                }

                TopicPartition tp = new TopicPartition(raw.topic(), raw.partition());
                currentOffsets.merge(tp, raw.offset() + 1, Long::max);
            }
        }

        if (timedOut) {
            logger.atWarn()
                    .log(
                            "Snapshot for topic [{}] completed with timeout — some items may have partial snapshot",
                            topic);
        }

        // Finalize all items in this pass
        for (SubscribedItem item : passItems) {
            finalizeItem(item);
        }
    }

    /**
     * Discovers all partitions for the target topic.
     *
     * @return list of TopicPartition objects, or empty if topic doesn't exist
     */
    private List<TopicPartition> discoverPartitions(Consumer<byte[], byte[]> consumer) {
        List<PartitionInfo> partitionInfos =
                consumer.partitionsFor(topic, Duration.ofMillis(30000));
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return Collections.emptyList();
        }
        return partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    /**
     * Polls the Kafka consumer with structured error handling.
     *
     * <p>Follows the same three-tier exception strategy used by {@link
     * com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper}:
     *
     * <ul>
     *   <li>{@link WakeupException}: re-thrown to signal graceful shutdown
     *   <li>{@link KafkaException}: logged and re-thrown as fatal (includes deserialization errors)
     *   <li>Unexpected exceptions: wrapped in {@code KafkaException} and re-thrown
     * </ul>
     *
     * @throws WakeupException if the consumer was woken up via {@link #shutdown()}
     * @throws KafkaException if a Kafka-level or deserialization error occurs
     */
    private ConsumerRecords<byte[], byte[]> doPoll(Consumer<byte[], byte[]> consumer) {
        try {
            return consumer.poll(POLL_TIMEOUT);
        } catch (WakeupException we) {
            logger.atDebug().log("Kafka Consumer woke up during snapshot poll");
            throw we;
        } catch (KafkaException ke) {
            logger.atError()
                    .setCause(ke)
                    .log("Unrecoverable exception during snapshot poll for topic [{}]", topic);
            throw ke;
        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Unexpected exception during snapshot poll for topic [{}]", topic);
            throw new KafkaException("Unexpected exception during snapshot poll", e);
        }
    }

    /**
     * Deserializes a raw {@link ConsumerRecord} into a typed {@link KafkaRecord}.
     *
     * <p>Uses eager deserialization since snapshot passes are single-threaded and records need to
     * be evaluated for routing.
     */
    private KafkaRecord<K, V> deserialize(ConsumerRecord<byte[], byte[]> raw) {
        K key =
                deserializerPair
                        .keyDeserializer()
                        .deserialize(raw.topic(), raw.headers(), raw.key());
        V value =
                raw.value() != null
                        ? deserializerPair
                                .valueDeserializer()
                                .deserialize(raw.topic(), raw.headers(), raw.value())
                        : null;
        return KafkaRecord.from(
                raw.topic(),
                raw.partition(),
                raw.offset(),
                raw.timestamp(),
                key,
                value,
                raw.headers());
    }

    /**
     * Checks if all partitions have been consumed up to the snapshot boundary.
     *
     * <p>A partition is considered consumed if the current offset has reached or exceeded the end
     * offset captured at pass start. Partitions with end offset 0 (empty) are considered consumed.
     */
    private static boolean allPartitionsConsumed(
            Map<TopicPartition, Long> currentOffsets, Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            long endOffset = entry.getValue();
            if (endOffset == 0) {
                continue; // Empty partition
            }
            long currentOffset = currentOffsets.getOrDefault(entry.getKey(), 0L);
            if (currentOffset < endOffset) {
                return false;
            }
        }
        return true;
    }

    /** Checks whether the pass has exceeded the configured timeout. */
    private boolean hasTimeoutElapsed(long passStartTime) {
        if (snapshotTimeout.isZero()) {
            return false; // No timeout configured
        }
        return (System.currentTimeMillis() - passStartTime) >= snapshotTimeout.toMillis();
    }

    /**
     * Creates a scoped {@link SubscribedItems} containing only the items in the current pass. This
     * ensures routing only matches against items being served, not all active subscriptions.
     */
    private static SubscribedItems scopedItems(Set<SubscribedItem> items) {
        SubscribedItems scoped = SubscribedItems.create();
        for (SubscribedItem item : items) {
            scoped.addItem(item);
        }
        return scoped;
    }

    /** Drains all pending items from the queue into a set. */
    private Set<SubscribedItem> drainPendingItems() {
        Set<SubscribedItem> drained = new LinkedHashSet<>();
        SubscribedItem item;
        while ((item = pendingItems.poll()) != null) {
            drained.add(item);
        }
        return drained;
    }

    /**
     * Drains all remaining pending items and finalizes them with an empty snapshot. Used as a
     * fallback when an error occurs — ensures items transition to real-time mode.
     */
    private void drainAndFinalizeAll() {
        Set<SubscribedItem> remaining = drainPendingItems();
        for (SubscribedItem item : remaining) {
            finalizeItem(item);
        }
    }

    /**
     * Completes snapshot delivery for a single item: signals end of snapshot and enables real-time
     * event dispatch.
     */
    private void finalizeItem(SubscribedItem item) {
        try {
            item.endOfSnapshot(eventListener);
            item.enableRealtimeEvents(eventListener);
            logger.atDebug().log("Snapshot finalized for item [{}]", item.name());
        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Error finalizing snapshot for item [{}]", item.name());
            // Enable real-time anyway to avoid permanently stuck items
            try {
                item.enableRealtimeEvents(eventListener);
            } catch (Exception inner) {
                logger.atError()
                        .setCause(inner)
                        .log("Failed to enable real-time events for item [{}]", item.name());
            }
        }
    }

    /**
     * Requests graceful shutdown of this snapshot consumer. Thread-safe; can be called from any
     * thread. The consumer will finish processing the current record and then exit via {@link
     * WakeupException}.
     *
     * <p>If the consumer has not yet started or has already completed, this method is a no-op.
     */
    public void shutdown() {
        Consumer<byte[], byte[]> consumer = this.activeConsumer;
        if (consumer != null) {
            logger.atDebug().log("Waking up snapshot consumer for topic [{}]", topic);
            consumer.wakeup();
        }
    }

    private void closeConsumer(Consumer<byte[], byte[]> consumer) {
        try {
            consumer.close();
            logger.atDebug().log("Snapshot consumer for topic [{}] closed", topic);
        } catch (Exception e) {
            logger.atWarn()
                    .setCause(e)
                    .log("Error closing snapshot consumer for topic [{}]", topic);
        }
    }
}
