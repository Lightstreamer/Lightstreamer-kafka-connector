
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

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordsBatch;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ParallelRecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.SingleThreadedRecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService;
import com.lightstreamer.kafka.test_utils.Mocks.MockOffsetService.ConsumedRecordInfo;
import com.lightstreamer.kafka.test_utils.Mocks.MockRecordProcessor;
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

    private static RecordMapper<String, String> newRecordMapper(Config<String, String> config) {
        return RecordMapper.<String, String>builder()
                .withCanonicalItemExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    private static ProcessUpdatesType getProcessUpdatesType(CommandModeStrategy commandMode) {
        ProcessUpdatesStrategy processUpdateStrategy =
                ProcessUpdatesStrategy.fromCommandModeStrategy(commandMode);
        return processUpdateStrategy.type();
    }

    private static final Logger logger = LogFactory.getLogger("TestConnection");

    private RecordConsumer<String, String> recordConsumer;
    private Config<String, String> config;
    private RecordMapper<String, String> recordMapper;
    private Items.SubscribedItems subscriptions;

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

        this.config = (Config<String, String>) connectorConfigurator.consumerConfig();

        this.subscriptions = SubscribedItems.create();

        // Configure the RecordMapper.
        this.recordMapper = newRecordMapper(config);
    }

    void subscribe(EventListener listener) {
        SubscribedItem item = Items.subscribedFrom("item", new Object());
        this.subscriptions.addItem(item);
        item.enableRealtimeEvents(listener);
    }

    private RecordConsumer<String, String> mkRecordConsumer(
            EventListener listener,
            int threads,
            CommandModeStrategy commandStrategy,
            OrderStrategy orderStrategy) {
        return RecordConsumer.<String, String>recordMapper(recordMapper)
                .subscribedItems(subscriptions)
                .commandMode(commandStrategy)
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
            this.recordConsumer.terminate();
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
    public void shouldBuildParallelRecordConsumerFromRecordMapperWithDefaultValues() {
        MockOffsetService offsetService = new MockOffsetService();
        EventListener listener = EventListener.smartEventListener(new MockItemEventListener());

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .commandMode(CommandModeStrategy.NONE) // Default value
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .errorStrategy(
                                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE) // Default value
                        .logger(logger)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isTrue();

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.recordProcessor)
                .isInstanceOf(DefaultRecordProcessor.class);
        // Default values
        assertThat(parallelRecordConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE);
        assertThat(parallelRecordConsumer.orderStrategy())
                .hasValue(OrderStrategy.ORDER_BY_PARTITION);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(1);
        assertThat(parallelRecordConsumer.actualThreads)
                .isEqualTo(parallelRecordConsumer.configuredThreads);
        assertThat(recordConsumer.numOfThreads()).isEqualTo(parallelRecordConsumer.actualThreads);

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) parallelRecordConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    @Test
    public void shouldBuildParallelRecordConsumerFromRecordProcessorWithDefaultValues() {
        MockOffsetService offsetService = new MockOffsetService();

        recordConsumer =
                RecordConsumer.<String, String>recordProcessor(new MockRecordProcessor<>())
                        .offsetService(offsetService)
                        .errorStrategy(
                                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE) // Default value
                        .logger(logger)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isTrue();

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
        // Default values
        assertThat(parallelRecordConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE);
        assertThat(parallelRecordConsumer.orderStrategy())
                .hasValue(OrderStrategy.ORDER_BY_PARTITION);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(1);
        assertThat(parallelRecordConsumer.actualThreads)
                .isEqualTo(parallelRecordConsumer.configuredThreads);
        assertThat(recordConsumer.numOfThreads()).isEqualTo(parallelRecordConsumer.actualThreads);
    }

    static Stream<Arguments> parallelConsumerArgs() {
        return Stream.of(
                arguments(
                        -1,
                        OrderStrategy.ORDER_BY_KEY,
                        CommandModeStrategy.AUTO,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE),
                arguments(
                        -1,
                        OrderStrategy.ORDER_BY_KEY,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION),
                arguments(
                        2,
                        OrderStrategy.ORDER_BY_PARTITION,
                        CommandModeStrategy.AUTO,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION),
                arguments(
                        2,
                        OrderStrategy.ORDER_BY_PARTITION,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE),
                arguments(
                        4,
                        OrderStrategy.UNORDERED,
                        CommandModeStrategy.AUTO,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION),
                arguments(
                        4,
                        OrderStrategy.UNORDERED,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE));
    }

    @ParameterizedTest
    @MethodSource("parallelConsumerArgs")
    public void shouldBuildParallelRecordConsumerFromRecordMapperWithNonDefaultValues(
            int threads,
            OrderStrategy order,
            CommandModeStrategy command,
            RecordErrorHandlingStrategy error) {
        MockOffsetService offsetService = new MockOffsetService();
        EventListener listener = EventListener.smartEventListener(new MockItemEventListener());

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .commandMode(command)
                        .eventListener(listener)
                        .offsetService(offsetService)
                        .errorStrategy(error)
                        .logger(logger)
                        .threads(threads)
                        .ordering(order)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isTrue();

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.recordProcessor)
                .isInstanceOf(DefaultRecordProcessor.class);
        // Non-default values
        assertThat(parallelRecordConsumer.errorStrategy()).isEqualTo(error);
        assertThat(parallelRecordConsumer.orderStrategy()).hasValue(order);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(threads);
        if (threads == -1) {
            assertThat(parallelRecordConsumer.actualThreads).isGreaterThan(1);
        } else {
            assertThat(parallelRecordConsumer.actualThreads)
                    .isEqualTo(parallelRecordConsumer.configuredThreads);
        }
        assertThat(recordConsumer.numOfThreads()).isEqualTo(parallelRecordConsumer.actualThreads);

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) parallelRecordConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(getProcessUpdatesType(command));
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    @ParameterizedTest
    @MethodSource("parallelConsumerArgs")
    public void shouldBuildParallelRecordConsumerFromRecordProcessorWithNonDefaultValues(
            int threads,
            OrderStrategy order,
            CommandModeStrategy commandMode,
            RecordErrorHandlingStrategy error) {
        MockOffsetService offsetService = new MockOffsetService();

        recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<>(getProcessUpdatesType(commandMode)))
                        .offsetService(offsetService)
                        .errorStrategy(error)
                        .logger(logger)
                        .threads(threads)
                        .ordering(order)
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(ParallelRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isTrue();

        ParallelRecordConsumer<String, String> parallelRecordConsumer =
                (ParallelRecordConsumer<String, String>) recordConsumer;
        assertThat(parallelRecordConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(parallelRecordConsumer.logger).isSameInstanceAs(logger);
        assertThat(parallelRecordConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
        // Non-default values
        assertThat(parallelRecordConsumer.errorStrategy()).isEqualTo(error);
        assertThat(parallelRecordConsumer.orderStrategy()).hasValue(order);
        assertThat(parallelRecordConsumer.configuredThreads).isEqualTo(threads);
        if (threads == -1) {
            assertThat(parallelRecordConsumer.actualThreads).isGreaterThan(1);
        } else {
            assertThat(parallelRecordConsumer.actualThreads)
                    .isEqualTo(parallelRecordConsumer.configuredThreads);
        }
        assertThat(recordConsumer.numOfThreads()).isEqualTo(parallelRecordConsumer.actualThreads);
    }

    @ParameterizedTest
    @EnumSource(CommandModeStrategy.class)
    public void shouldBuildSingleThreadedRecordConsumerFromRecordMapper(
            CommandModeStrategy commandMode) {
        MockOffsetService offsetService = new MockOffsetService();
        EventListener listener = EventListener.smartEventListener(new MockItemEventListener());

        recordConsumer =
                RecordConsumer.<String, String>recordMapper(recordMapper)
                        .subscribedItems(subscriptions)
                        .commandMode(commandMode)
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
        assertThat(recordConsumer.isParallel()).isFalse();
        assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
        assertThat(recordConsumer.orderStrategy()).isEmpty();

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor).isInstanceOf(DefaultRecordProcessor.class);

        DefaultRecordProcessor<String, String> recordProcessor =
                (DefaultRecordProcessor<String, String>) monoThreadedConsumer.recordProcessor;
        assertThat(recordProcessor.recordMapper).isSameInstanceAs(recordMapper);
        assertThat(recordProcessor.listener).isSameInstanceAs(listener);
        assertThat(recordProcessor.processUpdatesType())
                .isEqualTo(getProcessUpdatesType(commandMode));
        assertThat(recordProcessor.logger).isSameInstanceAs(logger);
        assertThat(recordProcessor.subscribedItems).isSameInstanceAs(subscriptions);
    }

    @ParameterizedTest
    @EnumSource(CommandModeStrategy.class)
    public void shouldBuildSingleThreadedRecordConsumerFromRecordProcessor(
            CommandModeStrategy commandMode) {
        MockOffsetService offsetService = new MockOffsetService();

        recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<>(getProcessUpdatesType(commandMode)))
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(1)
                        .preferSingleThread(
                                true) // This triggers a SingleThreadedRecordConsumer instance
                        .build();

        assertThat(recordConsumer).isNotNull();
        assertThat(recordConsumer).isInstanceOf(SingleThreadedRecordConsumer.class);
        assertThat(recordConsumer.isParallel()).isFalse();
        assertThat(recordConsumer.numOfThreads()).isEqualTo(1);
        assertThat(recordConsumer.orderStrategy()).isEmpty();

        SingleThreadedRecordConsumer<String, String> monoThreadedConsumer =
                (SingleThreadedRecordConsumer<String, String>) recordConsumer;
        assertThat(monoThreadedConsumer.offsetService).isSameInstanceAs(offsetService);
        assertThat(monoThreadedConsumer.logger).isSameInstanceAs(logger);
        assertThat(monoThreadedConsumer.errorStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
        assertThat(monoThreadedConsumer.recordProcessor).isInstanceOf(MockRecordProcessor.class);
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
                                    .commandMode(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("CommandModeStrategy not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("EventListener not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("OffsetService not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("ErrorStrategy not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("Logger not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(logger)
                                    .ordering(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("OrderStrategy not set");

        ne =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            RecordConsumer.<String, String>recordProcessor(null);
                        });
        assertThat(ne).hasMessageThat().isEqualTo("RecordProcessor not set");
    }

    @Test
    public void shouldFailBuildingDueToIllegalValues() {
        IllegalArgumentException ie =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.NONE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(logger)
                                    .threads(0)
                                    .build();
                        });
        assertThat(ie).hasMessageThat().isEqualTo("Threads number must be greater than zero");

        ie =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.ENFORCE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(logger)
                                    .threads(2)
                                    .build();
                        });
        assertThat(ie)
                .hasMessageThat()
                .isEqualTo("Command mode does not support parallel processing");

        ie =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            RecordConsumer.<String, String>recordMapper(recordMapper)
                                    .subscribedItems(subscriptions)
                                    .commandMode(CommandModeStrategy.ENFORCE)
                                    .eventListener(
                                            EventListener.smartEventListener(
                                                    new MockItemEventListener()))
                                    .offsetService(new MockOffsetService())
                                    .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                                    .logger(logger)
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
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", numOfRecords, keys, 2);

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(
                        listener, threads, CommandModeStrategy.NONE, OrderStrategy.ORDER_BY_KEY);

        for (int i = 0; i < iterations; i++) {
            RecordsBatch batch = recordConsumer.consumeRecords(records);
            batch.join();
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords);

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
        ConsumerRecords<byte[], byte[]> allRecords = new ConsumerRecords<>(recordsByPartition);

        // Create a list to store all the delivered events
        // List<Event> deliveredEvents = Collections.synchronizedList(new ArrayList<>());

        // Make the RecordConsumer
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(
                        listener,
                        threads,
                        CommandModeStrategy.NONE,
                        OrderStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            RecordsBatch batch = recordConsumer.consumeRecords(allRecords);
            batch.join();
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords * 2);
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
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", numOfRecords, keys, 3);

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(
                        listener,
                        threads,
                        CommandModeStrategy.NONE,
                        OrderStrategy.ORDER_BY_PARTITION);

        for (int i = 0; i < iterations; i++) {
            RecordsBatch batch = recordConsumer.consumeRecords(records);
            batch.join();
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
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", numOfRecords, keys, 3);

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(listener, 2, CommandModeStrategy.NONE, OrderStrategy.UNORDERED);

        for (int i = 0; i < iterations; i++) {
            RecordsBatch batch = recordConsumer.consumeRecords(records);
            batch.join();
            assertThat(deliveredEvents.size()).isEqualTo(numOfRecords);
            // Reset the listener for next iteration
            testListener.reset();
        }
    }

    @Test
    public void shouldConsumeFiltered() {
        // Generate records distributed into three partitions
        List<String> keys = Collections.emptyList();
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", 99, keys, 3);

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(listener, 2, CommandModeStrategy.NONE, OrderStrategy.UNORDERED);

        // Consume only the records published to partition 0
        RecordsBatch batch =
                recordConsumer.consumeFilteredRecords(records, c -> c.partition() == 0);
        batch.join();
        // Verify that only 1/3 of total published record have been consumed
        assertThat(testListener.getSmartRealtimeUpdates().size()).isEqualTo(records.count() / 3);
    }

    @Test
    public void shouldConsumeNullValues() {
        ConsumerRecord<byte[], byte[]> recordWithNullValue =
                new ConsumerRecord<>(
                        "topic", 0, 0L, "key".getBytes(StandardCharsets.UTF_8), null); // Null value
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition =
                new HashMap<>();
        recordsByPartition.put(new TopicPartition("topic", 0), List.of(recordWithNullValue));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsByPartition);

        // Make the RecordConsumer.
        MockItemEventListener testListener = new MockItemEventListener();
        EventListener listener = EventListener.smartEventListener(testListener);
        subscribe(listener);
        recordConsumer =
                mkRecordConsumer(listener, 2, CommandModeStrategy.NONE, OrderStrategy.UNORDERED);

        recordConsumer.consumeRecords(KafkaRecord.listFromDeferred(records, deserializerPair));
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
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<>(exception, offendingOffsets))
                        .offsetService(offsetService)
                        .errorStrategy(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION)
                        .logger(logger)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsumer if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        if (numOfThreads == 1) {
            assertThrows(KafkaException.class, () -> recordConsumer.consumeRecords(records));
        } else {
            // With multiple threads, exception may or may not be thrown depending on timing
            try {
                recordConsumer.consumeRecords(records);
            } catch (KafkaException expected) {
                logger.atError()
                        .log(
                                "KafkaException caught as expected when consuming records with multiple threads");
            }
        }
        // Ensure graceful shutdown of internal resources like ring buffers and executor threads.
        recordConsumer.close();

        List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();
        // With a single thread, processing stops at the first failure (offset 2l on partition 0),
        // so only offsets 0l and 1l are processed before the exception is thrown.
        // With multiple threads, processing continues despite failures, so all records except
        // the offending ones (which throw exceptions) are processed.
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
        ConsumerRecords<byte[], byte[]> records = generateRecords("topic", 30, keys, 2);

        // Prepare the list of offsets that will trigger a ValueException upon processing
        List<ConsumedRecordInfo> offendingOffsets =
                List.of(
                        new ConsumedRecordInfo("topic", 0, 2l),
                        new ConsumedRecordInfo("topic", 0, 4l),
                        new ConsumedRecordInfo("topic", 1, 3l));

        MockOffsetService offsetService = new MockOffsetService();
        recordConsumer =
                RecordConsumer.<String, String>recordProcessor(
                                new MockRecordProcessor<>(exception, offendingOffsets))
                        .offsetService(offsetService)
                        // The following prevents the exception to be propagated
                        .errorStrategy(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE)
                        .logger(logger)
                        .threads(numOfThreads)
                        // This enforces usage of the SingleThreadedConsume if numOfThreads is 1
                        .preferSingleThread(true)
                        .build();

        if (numOfThreads == 1) {
            if (exception instanceof ValueException) {
                // No exception should be thrown
                recordConsumer.consumeRecords(records);
            } else {
                // Other kind of exceptions should still be propagated
                assertThrows(KafkaException.class, () -> recordConsumer.consumeRecords(records));
            }
        } else {
            // With multiple threads, ValueExceptions should be ignored, other exceptions may still
            // be thrown depending on timing
            try {
                recordConsumer.consumeRecords(records);
            } catch (KafkaException expected) {
                logger.atError()
                        .log(
                                "KafkaException caught as expected when consuming records with multiple threads");
            }
        }

        recordConsumer.close();
        List<ConsumedRecordInfo> consumedRecords = offsetService.getConsumedRecords();

        if (exception instanceof ValueException) {
            // Ensure that all offsets are committed (even the offending ones)
            assertThat(consumedRecords).hasSize(records.count());
        } else {
            int expectedNumOfProcessedRecords =
                    numOfThreads == 1 ? 2 : records.count() - offendingOffsets.size();
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
