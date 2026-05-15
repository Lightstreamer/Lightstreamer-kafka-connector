
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
import static com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy.ORDER_BY_PARTITION;
import static com.lightstreamer.kafka.test_utils.Records.generateRecords;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.AbstractRecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ParallelRecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordProcessorImpl;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.SingleThreadedRecordConsumer;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;
import com.lightstreamer.kafka.test_utils.Mocks.MockRecordMapper;
import com.lightstreamer.kafka.test_utils.Mocks.UpdateCall;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
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
            ConnectionSpec<String, String> spec) {
        return RecordMapper.<String, String>builder()
                .withCanonicalItemExtractors(spec.itemTemplates().groupExtractors())
                .withFieldExtractor(spec.fieldsExtractor())
                .build();
    }

    private static ProcessUpdatesType getProcessUpdatesType(CommandMode commandMode) {
        return ProcessUpdatesStrategy.fromCommandMode(commandMode).type();
    }

    private static final Logger logger = LogFactory.getLogger("TestConnection");
    private static Monitor monitor = new Mocks.MockMonitor();

    private RecordConsumer<String, String> recordConsumer;
    private ConnectionSpec<String, String> connectionSpec;
    private RecordMapper<String, String> recordMapper;
    private Items.OnDemandSubscribedItems subscriptions;

    private DeserializerPair<String, String> deserializerPair =
            new DeserializerPair<>(String().deserializer(), String().deserializer());

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

        this.connectionSpec =
                (ConnectionSpec<String, String>) connectorConfigurator.connectionSpec();

        this.subscriptions = SubscribedItems.onDemand();

        // Configure the RecordMapper.
        this.recordMapper = newRecordMapper(connectionSpec);
    }

    void subscribeTo(String itemName) {
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem(itemName, new Object());
        this.subscriptions.addItem(item);
    }

    private RecordConsumer<String, String> mkRecordConsumer(
            ItemEventListener listener,
            int threads,
            CommandMode commandStrategy,
            OrderStrategy orderStrategy, boolean enableCatchUp) {
        return RecordConsumer.<String, String>recordMapper(recordMapper)
                .subscribedItems(subscriptions)
                .eventListener(listener)
                .offsetService(new MockOffsetService())
                .logger(logger)
                .errorStrategy(connectionSpec.errorHandlingStrategy())
                .commandMode(commandStrategy)
                .threads(threads)
                .ordering(orderStrategy)
                .enableCatchUp(enableCatchUp)
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
        ConsumerRecords<byte[], byte[]> records =
                generateRecords("topic", 40, List.of("a", "b", "c"));
        assertThat(records).hasSize(40);

        List<ConsumerRecord<byte[], byte[]>> recordByPartition =
                records.records(new TopicPartition("topic", 0));
        Map<String, List<Integer>> byKey =
                recordByPartition.stream()
                        .collect(
                                groupingBy(
                                        r ->
                                                deserializerPair
                                                        .keyDeserializer()
                                                        .deserialize("topic", r.key()),
                                        mapping(
                                                r ->
                                                        extractNumberedSuffix(
                                                                deserializerPair
                                                                        .valueDeserializer()
                                                                        .deserialize(
                                                                                "topic",
                                                                                r.value())),
                                                toList())));
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

    @Test
    public void shouldRecordConsumerWithDefaultValues() {
        MockOffsetService offsetService = new MockOffsetService();
        ItemEventListener listener = new MockItemEventListener();

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .logger(logger)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isTrue();

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService()).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.recordProcessor())
                .isInstanceOf(RecordProcessorImpl.class);

        // Default values
        assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
        assertThat(parallelRecordConsumer.ordering()).hasValue(ORDER_BY_PARTITION);
        assertThat(parallelRecordConsumer.enableCatchUp).isFalse();
        assertThat(parallelRecordConsumer.errorStrategy()).isEqualTo(IGNORE_AND_CONTINUE);
        assertThat(recordConsumer.monitor()).isNull();

        RecordProcessorImpl<String, String> recordProcessor =
                (RecordProcessorImpl<String, String>) parallelRecordConsumer.recordProcessor();
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    static Stream<Arguments> parallelConsumerArgs() {
        return Stream.of(
                arguments(
                        1, // Only one thread
                        true, // Prefer single thread to trigger SingleThreadedRecordConsumer
                        OrderStrategy.UNORDERED, // Actually irrelevant, since ordering is ignored for single-threaded consumers
                        true,
                        CommandMode.AUTO,
                        IGNORE_AND_CONTINUE,
                        monitor),
                arguments(
                        1,
                        false, // Trigger ParallelRecordConsumer even if only one thread
                        OrderStrategy.UNORDERED,
                        true,
                        CommandMode.EXPLICIT,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        monitor),                        
                arguments(
                        -1,
                        true, // Irrelevant for "threads" configured with -1 (auto)
                        OrderStrategy.ORDER_BY_KEY,
                        true,
                        CommandMode.AUTO,
                        IGNORE_AND_CONTINUE,
                        monitor),
                arguments(
                        -1,
                        false, // Irrelevant for "threads" configured with -1 (auto)
                        OrderStrategy.ORDER_BY_KEY,
                        true,
                        CommandMode.DISABLED,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        monitor),
                arguments(
                        2,
                        false, // Irrelevant when "threads" is greater than 1, since a ParallelRecordConsumer is created anyway
                        ORDER_BY_PARTITION,
                        false,
                        CommandMode.AUTO,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        monitor),
                arguments(
                        2,
                        false,
                        ORDER_BY_PARTITION,
                        true,
                        CommandMode.DISABLED,
                        IGNORE_AND_CONTINUE,
                        monitor),

                arguments(
                        4,
                        false,
                        OrderStrategy.UNORDERED,
                        false,
                        CommandMode.DISABLED,
                        IGNORE_AND_CONTINUE,
                        monitor));
    }

    @ParameterizedTest
    @MethodSource("parallelConsumerArgs")
    public void shouldBuildParallelRecordConsumerWithNonDefaultValues(
            int threads,
            boolean preferSingleThread,
            OrderStrategy order,
            boolean enableCatchUp,
            CommandMode command,
            RecordErrorHandlingStrategy error,
            Monitor monitor) {
        MockOffsetService offsetService = new MockOffsetService();
        ItemEventListener listener = new MockItemEventListener();

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .logger(logger)
                        .commandMode(command)
                        .errorStrategy(error)
                        .threads(threads)
                        .preferSingleThread(preferSingleThread)
                        .ordering(order)
                        .enableCatchUp(enableCatchUp)
                        .monitor(monitor)
                        .build();

        assertThat(recordConsumer).isNotNull();

        AbstractRecordConsumer<String, String> rc =
                (AbstractRecordConsumer<String, String>) recordConsumer;
        assertThat(rc.offsetService()).isSameInstanceAs(offsetService);
        assertThat(rc.logger).isSameInstanceAs(logger);
        assertThat(rc.recordProcessor()).isInstanceOf(RecordProcessorImpl.class);
        // Non-default values
        assertThat(rc.errorStrategy()).isEqualTo(error);

        assertThat(rc.enableCatchUp).isEqualTo(enableCatchUp);

        // These are the only strict conditions to get a SingleThreadedRecordConsumer, 
        // otherwise a ParallelRecordConsumer is created even if preferSingleThread is true 
        // and threads is 1.
        if (preferSingleThread && threads == 1) { 
            assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);
            assertThat(recordConsumer.isParallel()).isFalse();
            assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
            assertThat(recordConsumer.ordering()).isEmpty();
        } else {
            assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
            assertThat(recordConsumer.isParallel()).isTrue();

            if (threads == -1) {
                assertThat(rc.numOfThreads()).isGreaterThan(1);
            } else {
                assertThat(rc.numOfThreads()).isEqualTo(threads);
            }
            assertThat(rc.ordering()).hasValue(order);
        }

        assertThat(rc.monitor()).isSameInstanceAs(monitor);

        RecordProcessorImpl<String, String> recordProcessor =
                (RecordProcessorImpl<String, String>) recordConsumer.recordProcessor();
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(getProcessUpdatesType(command));
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    @ParameterizedTest
    @EnumSource(CommandMode.class)
    public void shouldBuildSingleThreadedRecordConsumer(CommandMode commandMode) {
        MockOffsetService offsetService = new MockOffsetService();
        ItemEventListener listener = new MockItemEventListener();

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .logger(logger)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .commandMode(commandMode)
                        .threads(1)
                        // The following should trigger a SingleThreadedRecordConsumer instance
                        .preferSingleThread(true)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isFalse();
        assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
        assertThat(recordConsumer.ordering()).isEmpty();

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService()).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor()).isInstanceOf(RecordProcessorImpl.class);

        RecordProcessorImpl<String, String> recordProcessor =
                (RecordProcessorImpl<String, String>) monoThreadedConsumer.recordProcessor();
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType())
                .isEqualTo(getProcessUpdatesType(commandMode));
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    public void shouldBuildSingleThreadedRecordConsumerWithNonDefaultValues(
            CommandMode commandMode) {
        MockOffsetService offsetService = new MockOffsetService();
        ItemEventListener listener = new MockItemEventListener();

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .logger(logger)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .commandMode(commandMode)
                        .threads(1)
                        // The following should trigger a SingleThreadedRecordConsumer instance
                        .preferSingleThread(true)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isFalse();
        assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
        assertThat(recordConsumer.ordering()).isEmpty();

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService()).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor()).isInstanceOf(RecordProcessorImpl.class);

        RecordProcessorImpl<String, String> recordProcessor =
                (RecordProcessorImpl<String, String>) monoThreadedConsumer.recordProcessor();
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType())
                .isEqualTo(getProcessUpdatesType(commandMode));
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    @Test
    public void shouldFailBuildingDueToNullValues() {
        NullPointerException ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("RecordMapper not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("SubscribedItems not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("EventListener not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("OffsetService not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("Logger not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(logger)
                                    .errorStrategy(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("ErrorStrategy not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(logger)
                                    .commandMode(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("CommandMode not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(logger)
                                    .ordering(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("OrderStrategy not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(logger)
                                    .monitor(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("Monitor not set");
    }

    @Test
    public void shouldFailBuildingDueToIllegalValues() {
        // Illegal values for threads: zero and negative numbers (except -1, which is a special
        // value to indicate "auto")
        int[] illegalThreadValues = {-2, 0};
        for (int threads : illegalThreadValues) {
            IllegalArgumentException ie =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> {
                                RecordConsumer.<String, String>recordMapper(recordMapper)
                                        .subscribedItems(subscriptions)
                                        .eventListener(new MockItemEventListener())
                                        .offsetService(new MockOffsetService())
                                        .logger(logger)
                                        .threads(threads)
                                        .build();
                            });
            assertThat(ie).hasMessageThat().isEqualTo("Threads number must be greater than zero");
        }

        // CommandMode.EXPLICIT does not support parallel processing, so threads must be 1
        illegalThreadValues = new int[] {-1, 2};
        for (int threads : illegalThreadValues) {
            IllegalArgumentException ie =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> {
                                RecordConsumer.<String, String>recordMapper(recordMapper)
                                        .subscribedItems(subscriptions)
                                        .eventListener(new MockItemEventListener())
                                        .offsetService(new MockOffsetService())
                                        .logger(logger)
                                        .commandMode(CommandMode.EXPLICIT)
                                        .threads(threads)
                                        .build();
                            });
            assertThat(ie)
                    .hasMessageThat()
                    .isEqualTo("Command mode does not support parallel processing");
        }

        IllegalArgumentException ie =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .eventListener(new MockItemEventListener())
                                    .offsetService(new MockOffsetService())
                                    .logger(logger)
                                    .commandMode(CommandMode.EXPLICIT)
                                    .threads(-1)
                                    .build();
                        });
        assertThat(ie)
                .hasMessageThat()
                .isEqualTo("Command mode does not support parallel processing");
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
    public void shouldDeliverKeyBasedOrder(int numOfRecords, int iterations, int threads)
            throws InterruptedException {
        // Generate records with keys "a", "b", "c", "d" and distribute them into 2 partitions of
        // the same topic
        List<String> keys = List.of("a", "b", "c", "d");
        ConsumerRecords<byte[], byte[]> consumerRecords =
                generateRecords("topic", numOfRecords, keys, 2);

        subscribeTo("item");

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(
                        testListener, threads, CommandMode.DISABLED, OrderStrategy.ORDER_BY_KEY, false);

        for (int i = 0; i < iterations; i++) {
            RecordBatch<String, String> batch =
                    RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
            recordConsumer.consumeBatch(batch);
            batch.join();
            List<Event> events =
                    testListener.getSmartRealtimeUpdates().stream()
                            .map(u -> buildEvent(u.event()))
                            .toList();

            for (String key : keys) {
                // Get the list of positions stored in all received events relative to the same key
                List<Integer> list =
                        events.stream()
                                .filter(e -> e.key().equals(key))
                                .map(Event::position)
                                .toList();
                // Ensure that positions (and, therefore, the events) relative to the same key are
                // in order
                assertThat(list).isInOrder();
            }
            // Reset the listener list for next iteration
            testListener.reset();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverPartitionBasedOrder(int numOfRecords, int iterations, int threads)
            throws InterruptedException {
        // Generate records with keys "a", "b", "c", "d" and distribute them into 2 topics
        List<String> keys = List.of("a", "b", "c", "d");

        // Provide less partitions than keys to enforce multiple key ending up to same partition
        int partitionsOnTopic1 = 3;
        int partitionsOnTopic2 = 2;

        // Generate records on different topics
        ConsumerRecords<byte[], byte[]> recordsOnTopic1 =
                generateRecords("topic1", numOfRecords, keys, partitionsOnTopic1);
        ConsumerRecords<byte[], byte[]> recordsOnTopic2 =
                generateRecords("topic2", numOfRecords, keys, partitionsOnTopic2);

        // Assemble an instance of ConsumerRecords with the generated records
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition =
                new HashMap<>();
        Consumer<? super ConsumerRecord<byte[], byte[]>> action =
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
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsByPartition);

        subscribeTo("item");

        // Make the RecordConsumer
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(testListener, threads, CommandMode.DISABLED, ORDER_BY_PARTITION, false);

        for (int i = 0; i < iterations; i++) {
            RecordBatch<String, String> batch =
                    RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
            recordConsumer.consumeBatch(batch);
            batch.join();
            List<Event> events =
                    testListener.getSmartRealtimeUpdates().stream()
                            .map(u -> buildEvent(u.event()))
                            .toList();
            for (int partition = 0; partition < partitionsOnTopic1; partition++) {
                assertDeliveredEventsOrder(partition, events, "topic1");
            }
            for (int partition = 0; partition < partitionsOnTopic2; partition++) {
                assertDeliveredEventsOrder(partition, events, "topic2");
            }
            // Reset the listener for next iteration
            testListener.reset();
        }
    }

    /**
     * Verifies that the events delivered for a specific partition and topic are in the correct
     * order based on their offsets.
     *
     * @param partition the partition number to check events for
     * @param deliveredEvents list of events that were delivered
     * @param topic the Kafka topic name to check events for
     */
    private void assertDeliveredEventsOrder(
            int partition, List<Event> deliveredEvents, String topic) {
        List<Number> offsets =
                deliveredEvents.stream()
                        .filter(e -> e.partition() == partition && e.topic().equals(topic))
                        .map(Event::offset)
                        .collect(toList());
        assertThat(offsets.size()).isGreaterThan(0);
        assertThat(offsets).isInOrder();
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverPartitionBasedOrderWithNoKey(
            int numOfRecords, int iterations, int threads) {
        List<String> keys = Collections.emptyList();
        ConsumerRecords<byte[], byte[]> consumerRecords =
                generateRecords("topic", numOfRecords, keys, 3);

        subscribeTo("item");

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(testListener, threads, CommandMode.DISABLED, ORDER_BY_PARTITION, false);

        for (int i = 0; i < iterations; i++) {
            RecordBatch<String, String> batch =
                    RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
            recordConsumer.consumeBatch(batch);
            batch.join();
            List<Event> events =
                    testListener.getSmartRealtimeUpdates().stream()
                            .map(u -> buildEvent(u.event()))
                            .toList();
            assertThat(events.size()).isEqualTo(numOfRecords);
            // Get the list of offsets per partition stored in all received events
            Map<String, List<Number>> byPartition = getByTopicAndPartition(events);

            // Ensure that the offsets relative to the same partition are in order
            Collection<List<Number>> orderedLists = byPartition.values();
            for (List<Number> orderedList : orderedLists) {
                assertThat(orderedList).isInOrder();
            }
            // Reset the listener for next iteration
            testListener.reset();
        }
    }

    @ParameterizedTest
    @MethodSource("iterations")
    public void shouldDeliverUnordered(int numOfRecords, int iterations, int threads)
            throws InterruptedException {
        List<String> keys = Collections.emptyList();
        ConsumerRecords<byte[], byte[]> consumerRecords =
                generateRecords("topic", numOfRecords, keys, 3);

        subscribeTo("item");

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(testListener, 2, CommandMode.DISABLED, OrderStrategy.UNORDERED, false);

        for (int i = 0; i < iterations; i++) {
            RecordBatch<String, String> batch =
                    RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
            recordConsumer.consumeBatch(batch);
            batch.join();
            List<UpdateCall> realtimeUpdates = testListener.getSmartRealtimeUpdates();
            List<Event> deliveredEvents =
                    realtimeUpdates.stream().map(u -> buildEvent(u.event())).toList();
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords);
            // Reset the listener for next iteration
            testListener.reset();
        }
    }

    @Test
    public void shouldConsumeNullValues() {
        ConsumerRecord<byte[], byte[]> recordWithNullValue =
                new ConsumerRecord<>(
                        "topic", 0, 0L, "key".getBytes(StandardCharsets.UTF_8), null); // Null value
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition =
                new HashMap<>();
        recordsByPartition.put(new TopicPartition("topic", 0), List.of(recordWithNullValue));
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsByPartition);

        subscribeTo("item");

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(testListener, 2, CommandMode.DISABLED, OrderStrategy.UNORDERED, false);

        RecordBatch<String, String> batch =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
        recordConsumer.consumeBatch(batch);
        batch.join();
        List<UpdateCall> realtimeUpdates = testListener.getSmartRealtimeUpdates();
        assertThat(realtimeUpdates).hasSize(1);
    }

    @Test
    public void shouldEndCatchUp() {
                ConsumerRecord<byte[], byte[]> recordWithNullValue =
                new ConsumerRecord<>(
                        "topic", 0, 0L, "key".getBytes(StandardCharsets.UTF_8), null); // Null value
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition =
                new HashMap<>();
        recordsByPartition.put(new TopicPartition("topic", 0), List.of(recordWithNullValue));
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsByPartition);

        subscribeTo("item");

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        recordConsumer =
                mkRecordConsumer(testListener, 2, CommandMode.DISABLED, OrderStrategy.UNORDERED, true);

        RecordBatch<String, String> batch =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
        recordConsumer.consumeBatch(batch);
        batch.join();
        List<UpdateCall> realtimeUpdates = testListener.getSmartRealtimeUpdates();
        assertThat(realtimeUpdates).hasSize(1);
        assertThat(realtimeUpdates.get(0).isSnapshot()).isTrue();
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
        ConsumerRecords<byte[], byte[]> consumerRecords = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        recordConsumer =
                RecordConsumer.<String, String>recordMapper(
                                new MockRecordMapper<>(exception, offendingOffsets))
                        .subscribedItems(subscriptions)
                        .eventListener(new MockItemEventListener())
                        .offsetService(offsetService)
                        .logger(logger)
                        // The following forces the exception to be propagated
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsumer if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        RecordBatch<String, String> batch =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
        if (numOfThreads == 1) {
            assertThrows(KafkaException.class, () -> recordConsumer.consumeBatch(batch));
        } else {
            // With multiple threads, exception may or may not be thrown depending on timing
            recordConsumer.consumeBatch(batch);
            batch.join();
            assertThat(recordConsumer.hasFailedAsynchronously()).isTrue();
        }
        // Ensure graceful shutdown of internal resources like ring buffers and executor threads.
        recordConsumer.close();

        List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();
        // With a single thread, processing stops at the first failure (offset 2l on partition 0),
        // so only offsets 0l and 1l are processed before the exception is thrown.
        // With multiple threads, processing continues despite failures, so all records except
        // the offending ones (which throw exceptions) are processed.
        int expectedNumOfProcessedRecords =
                numOfThreads == 1 ? 2 : consumerRecords.count() - offendingOffsets.size();
        assertThat(consumedRecords).hasSize(expectedNumOfProcessedRecords);
        assertThat(consumedRecords).containsNoneIn(offendingOffsets);
    }

    @ParameterizedTest
    @MethodSource("handleErrors")
    public void shouldIgnoreErrorsOnlyIfValueException(
            int numOfThreads, RuntimeException exception) {
        List<String> keys = List.of("a", "b");
        ConsumerRecords<byte[], byte[]> consumerRecords = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        recordConsumer =
                RecordConsumer.<String, String>recordMapper(
                                new MockRecordMapper<>(exception, offendingOffsets))
                        .subscribedItems(subscriptions)
                        .eventListener(new MockItemEventListener())
                        .offsetService(offsetService)
                        .logger(logger)
                        // The following prevents the exception from being propagated, but only if
                        // it's a ValueException.
                        // Other exceptions should still be propagated even with this strategy.
                        .errorStrategy(IGNORE_AND_CONTINUE)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsume if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        RecordBatch<String, String> batch =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, true);
        if (numOfThreads == 1) {
            if (exception instanceof ValueException) {
                // No exception should be thrown
                recordConsumer.consumeBatch(batch);
            } else {
                // Other kind of exceptions should still be propagated
                assertThrows(KafkaException.class, () -> recordConsumer.consumeBatch(batch));
                assertThat(recordConsumer.hasFailedAsynchronously()).isFalse();
            }
        } else {
            // With multiple threads, ValueExceptions should be ignored
            if (exception instanceof ValueException) {
                recordConsumer.consumeBatch(batch);
            } else {
                // On the other hand, other exceptions should still be propagated,
                // but can only be detected asynchronously
                recordConsumer.consumeBatch(batch);
                batch.join();
                assertThat(recordConsumer.hasFailedAsynchronously()).isTrue();
            }
        }

        recordConsumer.close();
        List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();

        if (exception instanceof ValueException) {
            // Ensure that all offsets are committed (even the offending ones)
            assertThat(consumedRecords).hasSize(consumerRecords.count());
        } else {
            int expectedNumOfProcessedRecords =
                    numOfThreads == 1 ? 2 : consumerRecords.count() - offendingOffsets.size();
            assertThat(consumedRecords).hasSize(expectedNumOfProcessedRecords);
            assertThat(consumedRecords).containsNoneIn(offendingOffsets);
        }
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

    /**
     * Creates a consumer function that builds {@code Event} objects from a map of strings. The
     * consumer function processes Kafka record information and adds new {@code Event} instances to
     * the provided list.
     *
     * @param events the list where constructed {@code Event} objects will be stored
     * @return A Consumer that processes maps containing Kafka record information with the following
     *     keys:
     *     <pre>
     *     - "topic": The Kafka topic
     *     - "key": The record key
     *     - "value": The record value, expected to be a string containing the key and a counter suffix (e.g., "a-3" -> "3")
     *     - "partition": The Kafka partition number
     *     - "offset": The record offset in the partition
     *     </pre>
     *     The consumer will create an Event object using these values along with the current thread
     *     name and add it to the provided events list.
     */
    private static BiConsumer<Map<String, String>, Boolean> buildEvent(List<Event> events) {
        return (map, isSnapshot) -> {
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

    /**
     * Converts a raw event map (typically obtained from {@code UpdateCall.event()}) into a
     * structured {@link Event} record for easier test assertions and ordering verification.
     *
     * <p>This method is used to transform the flat map representation of Kafka record data, as
     * captured by the {@link com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener}, into
     * a typed record that can be compared, sorted, and validated in tests.
     *
     * <p>The position field is extracted from the "value" by parsing its numeric suffix (e.g.,
     * "a-3" yields position 3), which allows tests to verify event ordering within a key or
     * partition.
     *
     * @param map a map containing the event properties with the following keys:
     *     <ul>
     *       <li>"topic" - the Kafka topic name
     *       <li>"key" - the record key
     *       <li>"value" - the record value, expected format "{key}-{number}" (e.g., "a-3")
     *       <li>"partition" - the partition number (as string, will be parsed to int)
     *       <li>"offset" - the offset value (as string, will be parsed to long)
     *     </ul>
     *
     * @return a new {@code Event} instance constructed from the map values and the current thread
     *     name, useful for verifying thread affinity in concurrent processing tests
     */
    private static Event buildEvent(Map<String, String> map) {
        String topic = map.get("topic");
        // Get the key
        String key = map.get("key");
        // Extract the position from the value: "a-3" -> "3"
        int position = extractNumberedSuffix(map.get("value"));
        // Get the partition
        String partition = map.get("partition");
        // Get the offset
        String offset = map.get("offset");
        // Create and return the event
        return new Event(
                topic,
                key,
                position,
                Integer.parseInt(partition),
                Long.parseLong(offset),
                Thread.currentThread().getName());
    }
}
