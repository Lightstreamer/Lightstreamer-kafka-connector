
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.Records.generateRecords;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ParallelRecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.SingleThreadedRecordConsumer;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;
import com.lightstreamer.kafka.test_utils.Mocks.MockRecordProcessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RecordConsumerTest {

    static record Event(
            String topic,
            String key,
            int position,
            int partition,
            long offset,
            String threadName) {}

    private static RecordMapper<String, String> newRecordMapper(
            ConsumerTriggerConfig<String, String> config) {
        return RecordMapper.<String, String>builder()
                .withTemplateExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    private static Logger logger = LogFactory.getLogger("connection");

    private RecordConsumer<String, String> recordConsumer;
    private ConsumerTriggerConfig<String, String> config;
    private RecordMapper<String, String> recordMapper;
    private Set<SubscribedItem> subscriptions;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() throws IOException {
        File adapterDir = Files.createTempDirectory("adapter_dir").toFile();
        Map<String, String> overrideSettings = new HashMap<>();
        overrideSettings.put("map.topic1.to", "item");
        overrideSettings.put("map.topic2.to", "item");
        overrideSettings.put("field.topic", "#{TOPIC}");
        overrideSettings.put("field.key", "#{KEY}");
        overrideSettings.put("field.value", "#{VALUE}");
        overrideSettings.put("field.partition", "#{PARTITION}");
        overrideSettings.put("field.offset", "#{OFFSET}");

        ConnectorConfigurator connectorConfigurator =
                new ConnectorConfigurator(
                        ConnectorConfigProvider.minimalConfigWith(overrideSettings), adapterDir);

        this.config = (ConsumerTriggerConfig<String, String>) connectorConfigurator.configure();

        String item = "item";
        this.subscriptions = Collections.singleton(Items.subscribedFrom(item, new Object()));

        // Configure the RecordMapper.
        this.recordMapper = newRecordMapper(config);
    }

    private RecordConsumer<String, String> mkRecordConsumer(
            ItemEventListener listener, int threads, OrderStrategy orderStrategy) {
        return RecordConsumer.<String, String>recordMapper(recordMapper)
                .subscribedItems(subscriptions)
                .eventListener(listener)
                .offsetService(new MockOffsetService())
                .errorStrategy(config.errorHandlingStrategy())
                .logger(logger)
                .threads(threads)
                .ordering(orderStrategy)
                .build();
    }

    @AfterEach
    public void tearDown() {
        if (this.recordConsumer != null) {
            this.recordConsumer.close();
        }
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
        assertThat(extractNumberedSuffix("EVENT-1")).isEqualTo(1);
    }

    @Test
    public void testRecordGeneration() {
        ConsumerRecords<String, String> records =
                generateRecords("topic", 40, List.of("a", "b", "c"));
        assertThat(records).hasSize(40);

        List<ConsumerRecord<String, String>> recordByPartition =
                records.records(new TopicPartition("topic", 0));
        Map<String, List<Integer>> byKey =
                recordByPartition.stream()
                        .collect(
                                groupingBy(
                                        ConsumerRecord::key,
                                        mapping(r -> extractNumberedSuffix(r.value()), toList())));
        Collection<List<Integer>> keyOrdered = byKey.values();
        for (List<Integer> list : keyOrdered) {
            assertThat(list).isInOrder();
        }
    }

    @ParameterizedTest
    @EnumSource
    public void shouldGetOrderStrategyFromConfig(RecordConsumeWithOrderStrategy orderStrategy) {
        assertThat(OrderStrategy.from(orderStrategy).toString())
                .isEqualTo(orderStrategy.toString());
    }

    @ParameterizedTest
    @EnumSource(RecordErrorHandlingStrategy.class)
    public void shouldBuildParallelRecordConsumerFromRecordMapperWithDefaultValues(
            RecordErrorHandlingStrategy error) {
        MockOffsetService offsetService = new MockOffsetService();
        MockItemEventListener listener = new MockItemEventListener();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .enforceCommandMode(false)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .errorStrategy(error)
                        .logger(logger)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.errorStrategy).isEqualTo(error);
        assertThat(parallelRecordConsumer.recordProcessor)
                .isInstanceOf(DefaultRecordProcessor.class);
        // Default values
        assertThat(parallelRecordConsumer.orderStrategy)
                .isEqualTo(OrderStrategy.ORDER_BY_PARTITION);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(1);
        assertThat(parallelRecordConsumer.actualThreads)
                .isEqualTo(parallelRecordConsumer.configuredThreads);

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) parallelRecordConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.log).isSameInstanceAs(logger);
    }

    @ParameterizedTest
    @EnumSource(RecordErrorHandlingStrategy.class)
    public void shouldBuildParallelRecordConsumerFromRecordProcessorDefaultValues(
            RecordErrorHandlingStrategy error) {
        MockOffsetService offsetService = new MockOffsetService();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<String, String>())
                        .offsetService(offsetService)
                        .errorStrategy(error)
                        .logger(logger)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.errorStrategy).isEqualTo(error);
        assertThat(parallelRecordConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
        // Default values
        assertThat(parallelRecordConsumer.orderStrategy)
                .isEqualTo(OrderStrategy.ORDER_BY_PARTITION);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(1);
        assertThat(parallelRecordConsumer.actualThreads)
                .isEqualTo(parallelRecordConsumer.configuredThreads);
    }

    static Stream<Arguments> parallelConsumerArgs() {
        return Stream.of(
                arguments(-1, OrderStrategy.ORDER_BY_KEY, true),
                arguments(2, OrderStrategy.ORDER_BY_PARTITION, true),
                arguments(4, OrderStrategy.UNORDERED, true));
    }

    @ParameterizedTest
    @MethodSource("parallelConsumerArgs")
    public void shouldBuildParallelRecordConsumerFromRecordMapperWithNonDefaultValues(
            int threads, OrderStrategy order, boolean preferSingleThread) {
        MockOffsetService offsetService = new MockOffsetService();
        MockItemEventListener listener = new MockItemEventListener();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(threads)
                        .ordering(order)
                        .preferSingleThread(preferSingleThread) // Pointless due to threads > 1
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.errorStrategy)
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(parallelRecordConsumer.recordProcessor)
                .isInstanceOf(DefaultRecordProcessor.class);
        // Non-default values
        assertThat(parallelRecordConsumer.orderStrategy).isEqualTo(order);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(threads);
        if (threads == -1) {
            assertThat(parallelRecordConsumer.actualThreads).isGreaterThan(1);
        } else {
            assertThat(parallelRecordConsumer.actualThreads)
                    .isEqualTo(parallelRecordConsumer.configuredThreads);
        }

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) parallelRecordConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.log).isSameInstanceAs(logger);
    }

    @ParameterizedTest
    @MethodSource("parallelConsumerArgs")
    public void shouldBuildParallelRecordConsumerFromRecordProcessorWithNonDefaultValues(
            int threads, OrderStrategy order, boolean preferSingleThread) {
        MockOffsetService offsetService = new MockOffsetService();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<String, String>())
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(threads)
                        .ordering(order)
                        .preferSingleThread(preferSingleThread) // Pointless due to threads > 1
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.errorStrategy)
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(parallelRecordConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
        // Non-default values
        assertThat(parallelRecordConsumer.orderStrategy).isEqualTo(order);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(threads);
        if (threads == -1) {
            assertThat(parallelRecordConsumer.actualThreads).isGreaterThan(1);
        } else {
            assertThat(parallelRecordConsumer.actualThreads)
                    .isEqualTo(parallelRecordConsumer.configuredThreads);
        }
    }

    @Test
    public void shouldBuildSingleThreadedRecordConsumerFromRecordMapper() {
        MockOffsetService offsetService = new MockOffsetService();
        MockItemEventListener listener = new MockItemEventListener();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(1)
                        // The following should trigger a SingleThreadedRecordConsumer instance
                        .preferSingleThread(true)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy)
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor).isInstanceOf(DefaultRecordProcessor.class);

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) monoThreadedConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.log).isSameInstanceAs(logger);
    }

    @Test
    public void shouldBuildSingleThreadedRecordConsumerFromRecordProcessor() {
        MockOffsetService offsetService = new MockOffsetService();

        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<String, String>())
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(1)
                        .preferSingleThread(
                                true) // This should trigger a SingleThreadedRecordConsumer instance
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy)
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
    }

    @Test
    public void shouldFailBuildingDueToNullValues() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecordConsumer.<String, String>recordMapper(recordMapper)
                            .subscribedItems(subscriptions)
                            .eventListener(null)
                            .offsetService(new MockOffsetService())
                            .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                            .logger(logger)
                            .build();
                });
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecordConsumer.<String, String>recordMapper(null)
                            .subscribedItems(subscriptions)
                            .eventListener(new MockItemEventListener())
                            .offsetService(new MockOffsetService())
                            .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                            .logger(logger)
                            .build();
                });
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecordConsumer.<String, String>recordMapper(recordMapper)
                            .subscribedItems(subscriptions)
                            .eventListener(new MockItemEventListener())
                            .offsetService(new MockOffsetService())
                            .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                            .logger(null)
                            .build();
                });
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecordConsumer.<String, String>recordProcessor(
                                    new MockRecordProcessor<String, String>())
                            .offsetService(new MockOffsetService())
                            .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                            .logger(null)
                            .build();
                });
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecordConsumer.<String, String>recordProcessor(null)
                            .offsetService(new MockOffsetService())
                            .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                            .logger(logger)
                            .build();
                });
    }

    @Test
    public void shouldFailBuildingDueToIllegalValues() {
        IllegalArgumentException ie =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(logger)
                                    .threads(0)
                                    .build();
                        });
        assertThat(ie.getMessage()).isEqualTo("Threads number must be greater than zero");
    }

    /**
     * Method source for the test methods.
     *
     * @return the number of records to generate, the number of iteration for each record
     *     collections, the number of threads to spin up
     */
    static Stream<Arguments> iterations() {
        return Stream.of(
                arguments(50, 1, 2),
                arguments(100, 1, 2),
                arguments(50, 1, 4),
                arguments(100, 1, 4),
                arguments(50, 10, 2),
                arguments(100, 10, 2),
                arguments(50, 10, 4),
                arguments(50, 10, 4),
                arguments(50, 100, 2),
                arguments(100, 100, 2),
                arguments(50, 100, 4),
                arguments(50, 100, 4),
                arguments(50, 1000, 2),
                arguments(100, 1000, 2),
                arguments(50, 1000, 4),
                arguments(100, 1000, 4));
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverKeyBasedOrder(int numOfRecords, int iterations, int threads) {
        // Generate records with keys "a", "b", "c", "d" and distribute them into 2 partitions of
        // the the same topic
        List<String> keys = List.of("a", "b", "c", "d");
        ConsumerRecords<String, String> records = generateRecords("topic", numOfRecords, keys, 2);

        // Create a list to store all the delivered events
        List<Event> deliveredEvents = Collections.synchronizedList(new ArrayList<>());
        // Make the RecordConsumer.
        recordConsumer =
                mkRecordConsumer(
                        new MockItemEventListener(buildEvent(deliveredEvents)),
                        threads,
                        OrderStrategy.ORDER_BY_KEY);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consumeRecords(records);
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords);

            for (String key : keys) {
                // Get the list of positions stored in all received events relative to the same key
                List<Integer> list =
                        deliveredEvents.stream()
                                .filter(e -> e.key().equals(key))
                                .map(Event::position)
                                .toList();
                // Ensure that positions (and, therefore, the events) relative to the same key are
                // in order
                assertThat(list).isInOrder();
            }
            // Reset the event list for next iteration
            deliveredEvents.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverPartitionBasedOrder(int numOfRecords, int iterations, int threads) {
        // Generate records with keys "a", "b", "c", "d" and distribute them into 2 topics
        List<String> keys = List.of("a", "b", "c", "d");
        // Provide less partitions than keys to enforce multiple key ending up to same partition.
        int partitionsOnTopic1 = 3;
        int partitionsOnTopic2 = 2;

        // Generate records on different topics
        ConsumerRecords<String, String> recordsOnTopic1 =
                generateRecords("topic1", numOfRecords, keys, partitionsOnTopic1);
        ConsumerRecords<String, String> recordsOnTopic2 =
                generateRecords("topic2", numOfRecords, keys, partitionsOnTopic2);

        // Create a new Consumer
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsByPartition =
                new HashMap<>();
        Consumer<? super ConsumerRecord<String, String>> action =
                consumerRecord ->
                        recordsByPartition.compute(
                                new TopicPartition(
                                        consumerRecord.topic(), consumerRecord.partition()),
                                (topicPartition, recordsList) -> {
                                    if (recordsList == null) {
                                        recordsList = new ArrayList<>();
                                    }
                                    recordsList.add(consumerRecord);
                                    return recordsList;
                                });
        recordsOnTopic1.forEach(action);
        recordsOnTopic2.forEach(action);

        ConsumerRecords<String, String> allRecords = new ConsumerRecords<>(recordsByPartition);

        // Make the RecordConsumer.
        List<Event> deliveredEvents = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new MockItemEventListener(buildEvent(deliveredEvents)),
                        threads,
                        OrderStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consumeRecords(allRecords);
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords * 2);
            for (int partition = 0; partition < partitionsOnTopic1; partition++) {
                final int p = partition;
                List<Number> offsets =
                        deliveredEvents.stream()
                                .filter(e -> e.partition() == p)
                                .filter(e -> e.topic().equals("topic1"))
                                .map(Event::offset)
                                .collect(toList());
                assertThat(offsets.size()).isGreaterThan(0);
                assertThat(offsets).isInOrder();
            }
            for (int partition = 0; partition < partitionsOnTopic2; partition++) {
                final int p = partition;
                List<Number> offsets =
                        deliveredEvents.stream()
                                .filter(e -> e.partition() == p)
                                .filter(e -> e.topic().equals("topic2"))
                                .map(Event::offset)
                                .collect(toList());
                assertThat(offsets.size()).isGreaterThan(0);
                assertThat(offsets).isInOrder();
            }
            // Reset the event list for next iteration
            deliveredEvents.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverPartitionBasedOrderWithNoKey(
            int numOfRecords, int iterations, int threads) {
        List<String> keys = Collections.emptyList();
        ConsumerRecords<String, String> records = generateRecords("topic", numOfRecords, keys, 3);

        // Make the RecordConsumer.
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new MockItemEventListener(buildEvent(events)),
                        threads,
                        OrderStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consumeRecords(records);
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Get the list of offsets per partition stored in all received events
            Map<String, List<Number>> byPartition = getByTopicAndPartition(events);

            // Ensure that the offsets relative to the same partition are in order
            Collection<List<Number>> orderedLists = byPartition.values();
            for (List<Number> orderedList : orderedLists) {
                assertThat(orderedList).isInOrder();
            }
            // Reset the event list for next iteration
            events.clear();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverUnordered(int numOfRecords, int iterations, int threads) {
        List<String> keys = Collections.emptyList();
        ConsumerRecords<String, String> records = generateRecords("topic", numOfRecords, keys, 3);

        // Make the RecordConsumer.
        List<Event> deliveredEvents = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new MockItemEventListener(buildEvent(deliveredEvents)),
                        2,
                        OrderStrategy.UNORDERED);

        for (int i = 0; i < iterations; i++) {
            recordConsumer.consumeRecords(records);
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords);
            // Reset the delivered events list for next iteration
            deliveredEvents.clear();
        }
    }

    @Test
    public void shouldConsumeFiltered() {
        // Generate records distributed into three partitions
        List<String> keys = Collections.emptyList();
        ConsumerRecords<String, String> records = generateRecords("topic", 99, keys, 3);

        // Make the RecordConsumer.
        List<Event> deliveredEvents = Collections.synchronizedList(new ArrayList<>());
        recordConsumer =
                mkRecordConsumer(
                        new MockItemEventListener(buildEvent(deliveredEvents)),
                        2,
                        OrderStrategy.UNORDERED);

        // Consume only the records published to partition 0
        recordConsumer.consumeFilteredRecords(records, c -> c.partition() == 0);
        // Verify that only 1/3 of total published record have been consumed
        assertThat(deliveredEvents.size()).isEqualTo(records.count() / 3);
    }

    static Stream<Arguments> handleErrors() {
        return Stream.of(
                Arguments.of(1, ValueException.fieldNotFound("field")),
                Arguments.of(2, ValueException.fieldNotFound("field")),
                Arguments.of(1, new RuntimeException("Serious issue")),
                Arguments.of(2, new RuntimeException("Serious issue")));
    }

    @ParameterizedTest
    @MethodSource("handleErrors")
    public void shouldHandleErrors(int numOfThreads, RuntimeException exception) {
        List<String> keys = List.of("a", "b");
        ConsumerRecords<String, String> records = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<String, String>(
                                        exception, offendingOffsets))
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsumer if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        assertThrows(KafkaException.class, () -> recordConsumer.consumeRecords(records));

        List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();
        // For single-threaded processing, processing will stop upon first failure, therefore only
        // the first two records (offsets 0l and 1l) will be processed.
        // For concurrent processing, processing won't stop upon first failure, therefore we expect
        // to find only the "good" offsets.
        int expectedNumOfProcessedRecords =
                numOfThreads == 1 ? 2 : records.count() - offendingOffsets.size();
        assertThat(consumedRecords).hasSize(expectedNumOfProcessedRecords);
        assertThat(consumedRecords).containsNoneIn(offendingOffsets);
    }

    @ParameterizedTest
    @MethodSource("handleErrors")
    public void shouldIgnoreErrorsOnlyIfValueException(
            int numOfThreads, RuntimeException exception) {
        List<String> keys = List.of("a", "b");
        ConsumerRecords<String, String> records = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        RecordConsumer<String, String> recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<String, String>(
                                        exception, offendingOffsets))
                        .offsetService(offsetService)
                        // The following prevents the exception to be propagated
                        .errorStrategy(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE)
                        .logger(logger)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsume if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        if (exception instanceof ValueException) {
            recordConsumer.consumeRecords(records);
            // Ensure that all offsets are committed (even the offending ones)
            List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();
            assertThat(consumedRecords).hasSize(records.count());
        } else {
            assertThrows(KafkaException.class, () -> recordConsumer.consumeRecords(records));
            List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();
            int expectedNumOfProcessedRecords =
                    numOfThreads == 1 ? 2 : records.count() - offendingOffsets.size();
            assertThat(consumedRecords).hasSize(expectedNumOfProcessedRecords);
            assertThat(consumedRecords).containsNoneIn(offendingOffsets);
        }
    }

    /**
     * Groups all the event offsets by key.
     *
     * @return a map of offsets list by key
     */
    private static Map<String, List<Number>> getByKey(List<Event> events) {
        return events.stream().collect(groupingBy(Event::key, mapping(Event::offset, toList())));
    }

    /**
     * Groups all the event offsets by topic and partition.
     *
     * @return a map of offsets list by topic and partition
     */
    private static Map<String, List<Number>> getByTopicAndPartition(List<Event> events) {
        return events.stream()
                .collect(
                        groupingBy(
                                e -> String.valueOf(e.topic() + "-" + e.partition()),
                                mapping(Event::offset, toList())));
    }

    private static Consumer<Map<String, String>> buildEvent(List<Event> events) {
        return map -> {
            String topic = map.get("topic");
            // Get the key
            String key = map.get("key");
            // Extract the position from the value: "a-3" -> "3"
            int position = extractNumberedSuffix(map.get("value"));
            // Get the partition
            String partition = map.get("partition");
            // Get the offset
            String offset = map.get("offset");
            // Create and add the event
            events.add(
                    new Event(
                            topic,
                            key,
                            position,
                            Integer.parseInt(partition),
                            Long.parseLong(offset),
                            Thread.currentThread().getName()));
        };
    }
}
