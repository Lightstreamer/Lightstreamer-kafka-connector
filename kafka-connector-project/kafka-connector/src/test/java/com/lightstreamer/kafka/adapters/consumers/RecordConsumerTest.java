
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

import com.lightstreamer.interfaces.data.DiffAlgorithm;
import com.lightstreamer.interfaces.data.IndexedItemEvent;
import com.lightstreamer.interfaces.data.ItemEvent;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.consumers.ConsumerLoop.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.ConsumerLoop.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.ConsumerLoop.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.ConsumerLoop.RecordConsumer.OrederStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RecordConsumerTest {

    record Event(String key, int position, int partition, long offset, String threadName) {}

    static class FakeOffsetService implements OffsetService {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

        @Override
        public void commitSync() {}

        @Override
        public void commitAsync() {}

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {}

        @Override
        public void onAsyncFailure(ConsumerRecord<?, ?> record, ValueException ve) {}

        @Override
        public ValueException getFirstFailure() {
            return null;
        }
    }

    static class FakteItemEventListener implements ItemEventListener {

        private Consumer<Map<String, String>> consumer;

        public FakteItemEventListener(Consumer<Map<String, String>> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void update(String itemName, ItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, Map event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void smartUpdate(Object itemHandle, ItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, Map event, boolean isSnapshot) {
            consumer.accept(event);
        }

        @Override
        public void smartUpdate(Object itemHandle, IndexedItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            throw new UnsupportedOperationException("Unimplemented method 'smartEndOfSnapshot'");
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            throw new UnsupportedOperationException("Unimplemented method 'smartClearSnapshot'");
        }

        @Override
        public void declareFieldDiffOrder(
                String itemName, Map<String, DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException("Unimplemented method 'declareFieldDiffOrder'");
        }

        @Override
        public void smartDeclareFieldDiffOrder(
                Object itemHandle, Map<String, DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'smartDeclareFieldDiffOrder'");
        }

        @Override
        public void failure(Throwable e) {
            throw new UnsupportedOperationException("Unimplemented method 'failure'");
        }
    }

    private static RecordMapper<String, String> newRecordMapper(
            ConsumerLoopConfig<String, String> config) {
        return RecordMapper.<String, String>builder()
                .withExtractor(config.itemTemplates().extractors())
                .withExtractor(config.fieldsExtractor())
                .build();
    }

    private static Logger log = LogFactory.getLogger("connection");

    private RecordConsumer<String, String> recordConsumer;

    private ConsumerLoopConfig<String, String> config;

    private RecordMapper<String, String> recordMapper;

    private Set<SubscribedItem> subscriptions;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() throws IOException {
        File adapterDir = Files.createTempDirectory("adapter_dir").toFile();
        Map<String, String> overrideSettings = new HashMap<>();
        overrideSettings.put("field.key", "#{KEY}");
        overrideSettings.put("field.value", "#{VALUE}");
        overrideSettings.put("field.partition", "#{PARTITION}");
        overrideSettings.put("field.offset", "#{OFFSET}");

        Map<String, String> params = ConnectorConfigProvider.minimalConfigWith(overrideSettings);
        ConnectorConfigurator connectorConfigurator = new ConnectorConfigurator(params, adapterDir);

        this.config = (ConsumerLoopConfig<String, String>) connectorConfigurator.configure();

        String item = "item";
        this.subscriptions = Collections.singleton(Items.susbcribedFrom(item, new Object()));

        // Configure the RecordMapper.
        this.recordMapper = newRecordMapper(config);
    }

    private RecordConsumer<String, String> mkRecordConsumer(
            ItemEventListener listener, int threads, OrederStrategy orederStrategy) {

        return new RecordConsumer.Builder<String, String>()
                .errorStrategy(config.recordErrorHandlingStrategy())
                .offsetService(new FakeOffsetService())
                .processor(
                        new DefaultRecordProcessor<String, String>(
                                recordMapper,
                                config.itemTemplates(),
                                config.fieldsExtractor(),
                                subscriptions,
                                listener,
                                log))
                .threads(threads, orederStrategy)
                .logger(log)
                .build();
    }

    @AfterEach
    public void tearDown() {
        if (this.recordConsumer != null) {
            this.recordConsumer.close();
        }
    }

    static ConsumerRecords<String, String> genRecords(String topic, int size, List<String> keys) {
        return genRecords(topic, size, keys, new int[] {0});
    }

    static ConsumerRecords<String, String> genRecords(
            String topic, int size, List<String> keys, int[] partitions) {

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        Map<String, Integer> eventCounter = new HashMap<>();
        Map<Integer, Integer> offsetCounter = new HashMap<>();
        SecureRandom secureRandom = new SecureRandom();

        // Generate the records list
        for (int i = 0; i < size; i++) {
            String recordKey = null;
            String recordValue = null;
            String eventCounterKey = "noKey";
            int partition = 0;

            if (keys.size() > 0) {
                // Select randomly one of the passed keys
                int index = secureRandom.nextInt(keys.size());
                recordKey = keys.get(index);
                eventCounterKey = recordKey;
                // Generate a value by adding a counter suffix to the key: key "a" -> value
                // "a-4"
                int counter = eventCounter.compute(eventCounterKey, (k, v) -> v == null ? 1 : ++v);
                recordValue = "%s-%d".formatted(recordKey, counter);
                // Select a partition based on the key hascode
                partition = recordKey.hashCode() % partitions.length;
            } else {
                // Generate a value simply by adding a counter suffix
                // Note that in this case the counter is global
                int counter = eventCounter.compute(eventCounterKey, (k, v) -> v == null ? 1 : ++v);
                recordValue = "%s-%d".formatted("EVENT", counter);
                // Round robin selection of the partion
                partition = i % partitions.length;
            }

            // Increment the offset relative to the selected partition
            int offset = offsetCounter.compute(partition, (p, o) -> o == null ? 0 : ++o);
            records.add(new ConsumerRecord<>(topic, partition, offset, recordKey, recordValue));
        }

        // Group records by partition to be passed to the ConsumerRecords instance
        Map<TopicPartition, List<ConsumerRecord<String, String>>> partionsToRecords =
                records.stream()
                        .collect(
                                groupingBy(
                                        record -> new TopicPartition("topic", record.partition()),
                                        mapping(Function.identity(), Collectors.toList())));
        return new ConsumerRecords<>(partionsToRecords);
    }

    static int extractNumberedSuffix(String value) {
        int i = value.indexOf("-");
        if (i != -1) {
            return Integer.parseInt(value.substring(i + 1));
        }
        throw new RuntimeException("Cannot extract number from " + value);
    }

    @Test
    public void testExtractNumberSuffix() {
        assertThat(extractNumberedSuffix("abc-21")).isEqualTo(21);
        assertThat(extractNumberedSuffix("EVENTT-1")).isEqualTo(1);
    }

    @Test
    public void testRecordGeneration() {
        ConsumerRecords<String, String> records = genRecords("topic", 40, List.of("a", "b", "c"));
        assertThat(records).hasSize(40);

        List<ConsumerRecord<String, String>> recordByPartition =
                records.records(new TopicPartition("topic", 0));
        Map<String, List<Integer>> byKey =
                recordByPartition.stream()
                        .collect(
                                groupingBy(
                                        ConsumerRecord::key,
                                        mapping(
                                                r -> extractNumberedSuffix(r.value()),
                                                Collectors.toList())));
        Collection<List<Integer>> keyOrdered = byKey.values();
        for (List<Integer> list : keyOrdered) {
            assertThat(list).isInOrder();
        }
    }

    /**
     * Method source for the test methods.
     *
     * @return the number of records to generate, the number of iteration for each record
     *     collections, the number of threads to spin up
     */
    static Stream<Arguments> iterations() {
        return Stream.of(
                arguments(100, 1, 4),
                arguments(200, 1, 4),
                arguments(100, 1, 8),
                arguments(200, 1, 8),
                arguments(100, 10, 4),
                arguments(200, 10, 4),
                arguments(100, 10, 8),
                arguments(100, 10, 8),
                arguments(100, 100, 4),
                arguments(200, 100, 4),
                arguments(100, 100, 8),
                arguments(100, 100, 8),
                arguments(100, 1000, 4),
                arguments(200, 1000, 4),
                arguments(100, 1000, 8),
                arguments(200, 1000, 8));
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shoudDeliverKeyBasedOrder(int numOfRecords, int iterations, int threads) {
        ConsumerRecords<String, String> records =
                genRecords("topic", numOfRecords, List.of("a", "b", "c", "d"), new int[] {0, 1});

        // Make the RecordConsumer.
        List<Event> events = Collections.synchronizedList(new ArrayList<>());

        recordConsumer =
                mkRecordConsumer(
                        new FakteItemEventListener(buildEvent(events)),
                        threads,
                        OrederStrategy.ORDER_BY_KEY);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consume(records);
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Group all the received events by record key
            Map<String, List<Integer>> byKey = getByKey(events);
            // Ensure that events relative to the same key are in order
            Collection<List<Integer>> orederedLists = byKey.values();
            for (List<Integer> orderedList : orederedLists) {
                assertThat(orderedList).isInOrder();
            }
            // Reset the event list for next iteration
            events.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shoudDeliverPartitionBasedOrder(int numOfRecords, int iterations, int threads) {
        List<String> keys = List.of("a", "b", "c");
        // Provide less partitions than keys to enforce multiple key ending up to same partition.
        int[] partitions = IntStream.range(0, keys.size() - 1).toArray();

        ConsumerRecords<String, String> records =
                genRecords("topic", numOfRecords, keys, partitions);

        // Make the RecordConsumer.
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new FakteItemEventListener(buildEvent(events)),
                        threads,
                        OrederStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consume(records);
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Get the list of offsets per partition stored in all received events
            Map<String, List<Long>> byPartition = getByPartition(events);

            // Ensure that the offsets relative to the same partition are in order
            Collection<List<Long>> orederedLists = byPartition.values();
            for (List<Long> orderedList : orederedLists) {
                assertThat(orderedList).isInOrder();
            }
            // Reset the event list for next iteration
            events.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shoudDeliverPartitionBasedOrderWithNoKey(
            int numOfRecords, int iterations, int threads) {
        List<String> keys = Collections.emptyList();
        int[] partitions = IntStream.range(0, 3).toArray();

        ConsumerRecords<String, String> records =
                genRecords("topic", numOfRecords, keys, partitions);

        // Make the RecordConsumer.
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new FakteItemEventListener(buildEvent(events)),
                        threads,
                        OrederStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consume(records);
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Get the list of offsets per partition stored in all received events
            Map<String, List<Long>> byPartition = getByPartition(events);

            // Ensure that the offsets relative to the same partition are in order
            Collection<List<Long>> orederedLists = byPartition.values();
            for (List<Long> orderedList : orederedLists) {
                assertThat(orderedList).isInOrder();
            }
            // Reset the event list for next iteration
            events.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shoudDeliverUnordered(int numOfRecords, int iterations, int threads) {
        List<String> keys = Collections.emptyList();
        int[] partitions = IntStream.range(0, 3).toArray();

        ConsumerRecords<String, String> records =
                genRecords("topic", numOfRecords, keys, partitions);

        // Make the RecordConsumer.
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new FakteItemEventListener(buildEvent(events)), 2, OrederStrategy.UNORDERD);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consume(records);
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Reset the event list for next iteration
            events.clear();
        }
    }

    private static Map<String, List<Integer>> getByKey(List<Event> events) {
        return events.stream()
                .collect(groupingBy(Event::key, mapping(Event::position, Collectors.toList())));
    }

    /**
     * Groups all the event offsets per partition.
     *
     * @return a map of offsets list per partition
     */
    private static Map<String, List<Long>> getByPartition(List<Event> events) {
        return events.stream()
                .collect(
                        groupingBy(
                                e -> String.valueOf(e.partition()),
                                mapping(Event::offset, Collectors.toList())));
    }

    private static Consumer<Map<String, String>> buildEvent(List<Event> events) {
        return event -> {
            // Get the key
            String key = event.get("key");
            // Extract the position from the value: "a-3" -> "3"
            int position = extractNumberedSuffix(event.get("value"));
            // Get the partition
            String partition = event.get("partition");
            // Get the offset
            String offset = event.get("offset");
            // Create and add the event
            events.add(
                    new Event(
                            key,
                            position,
                            Integer.parseInt(partition),
                            Long.parseLong(offset),
                            Thread.currentThread().getName()));
        };
    }

    public static void main(String[] args) {
        test(List.of("a", "b", "c"));
        // test(Collections.emptyList());
    }

    private static void test(List<String> keys) {
        ConsumerRecords<String, String> records = genRecords("topic", 40, keys, new int[] {0, 1});
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        while (iterator.hasNext()) {
            ConsumerRecord<String, String> record = iterator.next();
            Event event =
                    new Event(
                            record.key(),
                            extractNumberedSuffix(record.value()),
                            record.partition(),
                            record.offset(),
                            Thread.currentThread().getName());

            events.add(event);
            dump(record);
            // dump(event);
        }

        Map<String, List<Long>> byPartition = getByPartition(events);
        List<Long> list = byPartition.get("1");
        assertThat(list).isInOrder();
    }

    static void dump(Event event) {
        System.out.format(
                "parition = %d, offset = %d, key = %s, position = %d%n",
                event.partition(), event.offset(), event.key(), event.position());
    }

    static void dump(ConsumerRecord<?, ?> record) {
        System.out.format(
                "parition  %d, key = %s, value = %s%n",
                record.partition(), record.key(), record.value());
    }
}
