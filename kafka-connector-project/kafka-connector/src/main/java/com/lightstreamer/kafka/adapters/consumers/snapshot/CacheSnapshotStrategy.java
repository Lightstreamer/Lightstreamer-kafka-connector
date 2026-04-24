
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A {@link SnapshotDeliveryStrategy} backed by an in-memory cache that maintains a materialized
 * view of one or more compacted Kafka topics. Snapshot delivery is a synchronous {@link
 * ConcurrentHashMap} lookup — sub-millisecond.
 *
 * <p>The cache lifecycle has two phases:
 *
 * <ol>
 *   <li><b>Phase 1 (initial load)</b> — {@link #load()} reads every configured topic from beginning
 *       to end offsets, populating the cache synchronously. Called during adapter {@code init()},
 *       before any subscriptions arrive.
 *   <li><b>Phase 2 (continuous tailing)</b> — {@link #startSync()} spawns a dedicated background
 *       thread per topic that polls for new records and keeps the cache up to date.
 * </ol>
 *
 * <p>Tombstone records (null value) remove the corresponding entry from the cache, accurately
 * reflecting deletions in the compacted topic.
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 * @see SnapshotDeliveryStrategy
 */
public class CacheSnapshotStrategy<K, V> implements SnapshotDeliveryStrategy<K, V> {

    private static final Duration LOAD_POLL_TIMEOUT = Duration.ofMillis(5000);
    private static final Duration TAIL_POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration PARTITION_DISCOVERY_TIMEOUT = Duration.ofMillis(30000);
    private static final int PARTITION_DISCOVERY_MAX_RETRIES = 5;
    private static final Duration PARTITION_DISCOVERY_RETRY_INTERVAL = Duration.ofMillis(5000);

    private final Logger logger;
    private final RecordMapper<K, V> recordMapper;
    private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;
    private final KafkaRecord.DeserializerPair<K, V> deserializerPair;
    private final Set<String> topics;

    /**
     * The in-memory cache: canonical item name to the latest fields map. Updated by the load phase
     * and the tailing threads; read by {@link #deliverSnapshot}.
     */
    private final ConcurrentHashMap<String, Map<String, String>> cache = new ConcurrentHashMap<>();

    /** Per-topic tailing tasks, started after {@link #load()} completes. */
    private final Map<String, TailingTask> tailingTasks = new HashMap<>();

    /** Thread pool for running tailing tasks. Uses daemon threads, one per topic. */
    private final ExecutorService tailingPool;

    public CacheSnapshotStrategy(
            SnapshotConnectionSpec<K, V> connectionSpec,
            RecordMapper<K, V> recordMapper,
            Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
        this.logger = LogFactory.getLogger(connectionSpec.connectionName() + ".snapshot");
        this.recordMapper = recordMapper;
        this.consumerSupplier = consumerSupplier;
        this.deserializerPair = connectionSpec.deserializerPair();
        this.topics = connectionSpec.itemTemplates().topics();
        this.tailingPool =
                Executors.newFixedThreadPool(
                        topics.size(),
                        r -> {
                            Thread t = new Thread(r, "CacheTailer");
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public void init(ItemEventListener listener) throws KafkaException {
        logger.atInfo().log("Initializing snapshot cache for {} topics...", topics.size());
        Map<String, Map<TopicPartition, Long>> resumeOffsets = load();
        startSync(resumeOffsets);
    }

    /**
     * Loads the full compacted state from Kafka for all configured topics. Blocks until every
     * partition has been consumed up to its end offset. On failure, throws — the adapter should not
     * start.
     *
     * @return per-topic resume offsets for the tailing phase
     * @throws KafkaException if any Kafka operation fails during the load
     */
    Map<String, Map<TopicPartition, Long>> load() {
        long loadStart = System.currentTimeMillis();
        Map<String, Map<TopicPartition, Long>> resumeOffsets = new HashMap<>();
        for (String topic : topics) {
            resumeOffsets.put(topic, loadTopic(topic));
        }
        long loadElapsed = System.currentTimeMillis() - loadStart;
        logger.atInfo().log(
                "Snapshot cache loaded: {} entries across {} topics in {} ms",
                cache.size(),
                topics.size(),
                loadElapsed);
        return resumeOffsets;
    }

    /**
     * Starts a dedicated tailing thread per topic to keep the cache in sync with Kafka. Each task
     * receives its own resume offsets from the load phase, ensuring a gapless handoff.
     *
     * @param resumeOffsets per-topic partition offsets where the load phase stopped
     */
    void startSync(Map<String, Map<TopicPartition, Long>> resumeOffsets) {
        for (String topic : topics) {
            Map<TopicPartition, Long> offsets = resumeOffsets.getOrDefault(topic, Map.of());
            TailingTask tailer = new TailingTask(topic, offsets);
            tailingTasks.put(topic, tailer);
            tailingPool.submit(tailer);
        }
    }

    @Override
    public void deliverSnapshot(SubscribedItem item, EventListener listener) {
        Optional<Map<String, String>> cached = lookup(item);
        if (cached.isPresent()) {
            logger.atInfo().log("Delivering cached snapshot for item [{}]", item.name());
            item.sendSnapshotEvent(cached.get(), listener);
        } else {
            logger.atInfo().log(
                    "No cached data for item [{}], delivering empty snapshot", item.name());
        }
        // Always finalize — empty snapshot is valid
        item.endOfSnapshot(listener);
        item.enableRealtimeEvents(listener);
        logger.atInfo().log("Snapshot delivery completed for item [{}]", item.name());
    }

    /**
     * Looks up the cached snapshot data for the given item.
     *
     * @param item the subscribed item to look up
     * @return the latest fields map, or empty if the item is not in the cache
     */
    Optional<Map<String, String>> lookup(SubscribedItem item) {
        return Optional.ofNullable(cache.get(item.asCanonicalItemName()));
    }

    /**
     * Returns the current number of entries in the cache. Intended for testing and monitoring.
     *
     * @return the number of cached entries
     */
    int cacheSize() {
        return cache.size();
    }

    @Override
    public void shutdown() {
        tailingTasks.values().forEach(TailingTask::stop);
        tailingTasks.clear();
        tailingPool.shutdown();
        try {
            if (!tailingPool.awaitTermination(10, TimeUnit.SECONDS)) {
                tailingPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            tailingPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        cache.clear();
    }

    private Map<TopicPartition, Long> loadTopic(String topic) {
        logger.atInfo().log("Loading snapshot cache for topic [{}]...", topic);
        long topicStart = System.currentTimeMillis();
        Consumer<byte[], byte[]> consumer = consumerSupplier.get();
        try {
            List<TopicPartition> partitions = discoverPartitionsWithRetry(consumer, topic);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            long estimatedRecords =
                    endOffsets.entrySet().stream()
                            .mapToLong(
                                    e ->
                                            e.getValue()
                                                    - beginningOffsets.getOrDefault(e.getKey(), 0L))
                            .sum();
            logger.atInfo().log(
                    "Topic [{}]: {} partitions, ~{} records to consume (upper bound,"
                            + " actual count may be lower due to compaction)",
                    topic,
                    partitions.size(),
                    estimatedRecords);

            Map<TopicPartition, Long> currentOffsets = new HashMap<>();

            while (!allPartitionsConsumed(currentOffsets, endOffsets)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(LOAD_POLL_TIMEOUT);
                for (ConsumerRecord<byte[], byte[]> raw : records) {
                    processRecord(raw);
                    TopicPartition tp = new TopicPartition(raw.topic(), raw.partition());
                    currentOffsets.merge(tp, raw.offset() + 1, Long::max);
                }
                logger.atInfo().log(
                        "Processed {} records for topic [{}]...", records.count(), topic);
            }

            long topicElapsed = System.currentTimeMillis() - topicStart;
            logger.atInfo().log(
                    "Snapshot cache loaded for topic [{}]: {} total cache entries in {} ms",
                    topic,
                    cache.size(),
                    topicElapsed);
            return currentOffsets;
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> raw) {
        try {
            KafkaRecord<K, V> kafkaRecord = KafkaRecord.fromEager(raw, deserializerPair);
            if (kafkaRecord.isPayloadNull()) {
                // Tombstone: resolve canonical names, then remove
                MappedRecord mapped = recordMapper.map(kafkaRecord);
                for (String canonicalName : mapped.canonicalItemNames()) {
                    cache.remove(canonicalName);
                }
                return;
            }

            MappedRecord mapped = recordMapper.map(kafkaRecord);
            Map<String, String> fields = mapped.fieldsMap();
            for (String canonicalName : mapped.canonicalItemNames()) {
                cache.put(canonicalName, fields);
            }
        } catch (ValueException e) {
            logger.atWarn()
                    .setCause(e)
                    .log(
                            "Skipping record at offset {} on {}-{} due to extraction error",
                            raw.offset(),
                            raw.topic(),
                            raw.partition());
        }
    }

    private static List<TopicPartition> discoverPartitions(
            Consumer<byte[], byte[]> consumer, String topic) {
        List<PartitionInfo> partitionInfos =
                consumer.partitionsFor(topic, PARTITION_DISCOVERY_TIMEOUT);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return List.of();
        }
        return partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    /**
     * Discovers partitions for a topic, retrying if the topic is not yet available. Used during the
     * load phase, where the topic must eventually exist for the cache to function.
     *
     * @throws KafkaException if the topic is still unavailable after all retries
     */
    private List<TopicPartition> discoverPartitionsWithRetry(
            Consumer<byte[], byte[]> consumer, String topic) {
        for (int attempt = 1; attempt <= PARTITION_DISCOVERY_MAX_RETRIES; attempt++) {
            List<TopicPartition> partitions = discoverPartitions(consumer, topic);
            if (!partitions.isEmpty()) {
                return partitions;
            }
            logger.atWarn()
                    .log(
                            "No partitions found for topic [{}] (attempt {}/{}), retrying in {} ms...",
                            topic,
                            attempt,
                            PARTITION_DISCOVERY_MAX_RETRIES,
                            PARTITION_DISCOVERY_RETRY_INTERVAL.toMillis());
            try {
                Thread.sleep(PARTITION_DISCOVERY_RETRY_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaException(
                        "Interrupted while waiting for topic [" + topic + "] partitions", e);
            }
        }
        throw new KafkaException(
                "Topic ["
                        + topic
                        + "] has no partitions after "
                        + PARTITION_DISCOVERY_MAX_RETRIES
                        + " retries; cannot load snapshot cache");
    }

    private static boolean allPartitionsConsumed(
            Map<TopicPartition, Long> currentOffsets, Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            long endOffset = entry.getValue();
            if (endOffset == 0) {
                continue; // empty partition
            }
            long currentOffset = currentOffsets.getOrDefault(entry.getKey(), 0L);
            if (currentOffset < endOffset) {
                return false;
            }
        }
        return true;
    }

    /**
     * A task that continuously polls a single Kafka topic from its current position, updating the
     * shared cache with new records. Submitted to {@link #tailingPool}.
     */
    private class TailingTask implements Runnable {

        private final String topic;
        private final Map<TopicPartition, Long> resumeOffsets;

        private volatile boolean closed;
        private volatile Consumer<byte[], byte[]> activeConsumer;

        TailingTask(String topic, Map<TopicPartition, Long> resumeOffsets) {
            this.topic = topic;
            this.resumeOffsets = resumeOffsets;
        }

        @Override
        public void run() {
            Consumer<byte[], byte[]> consumer = consumerSupplier.get();
            this.activeConsumer = consumer;
            try {
                List<TopicPartition> partitions = discoverPartitions(consumer, topic);
                if (partitions.isEmpty()) {
                    logger.atWarn()
                            .log("No partitions found for topic [{}], tailing task exiting", topic);
                    return;
                }

                consumer.assign(partitions);
                // Resume from the exact offsets where the load phase stopped,
                // ensuring no records are missed during the handoff. Partitions not
                // seen during load (e.g., added after load completed) start from 0
                // to capture their full state.
                for (TopicPartition tp : partitions) {
                    consumer.seek(tp, resumeOffsets.getOrDefault(tp, 0L));
                }

                logger.atInfo().log("Snapshot cache tailing started for topic [{}]", topic);

                while (!closed) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(TAIL_POLL_TIMEOUT);
                    logger.atInfo().log(
                            "Polled {} new records for topic [{}]...", records.count(), topic);
                    for (ConsumerRecord<byte[], byte[]> raw : records) {
                        processRecord(raw);
                    }
                }
            } catch (WakeupException e) {
                logger.atDebug().log(
                        "Cache tailing task woken up for shutdown of topic [{}]", topic);
            } catch (Exception e) {
                logger.atError()
                        .setCause(e)
                        .log(
                                "Snapshot cache tailing failed for topic [{}], "
                                        + "snapshots may become stale",
                                topic);
            } finally {
                this.activeConsumer = null;
                consumer.close();
                logger.atDebug().log("Snapshot cache tailing stopped for topic [{}]", topic);
            }
        }

        void stop() {
            closed = true;
            Consumer<byte[], byte[]> c = activeConsumer;
            if (c != null) {
                c.wakeup();
            }
        }
    }
}
