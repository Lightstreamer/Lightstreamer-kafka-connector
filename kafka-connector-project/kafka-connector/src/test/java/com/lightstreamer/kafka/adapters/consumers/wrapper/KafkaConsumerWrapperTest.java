
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecords;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class KafkaConsumerWrapperTest {

    private final OffsetResetStrategy resetStrategy = OffsetResetStrategy.EARLIEST;
    private MockMetadataListener metadataListener = new MockMetadataListener();
    private MockItemEventListener itemEventListener = new MockItemEventListener();
    private EventListener eventListener = EventListener.smartEventListener(itemEventListener);
    private MockConsumer<Deferred<String>, Deferred<String>> mockConsumer =
            new MockConsumer<>(resetStrategy);

    private Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                resetStrategy.equals(EARLIEST) ? "earliest" : "latest");
        return properties;
    }

    private TopicConfigurations makeTopicsConfig(boolean enableSubscriptionPattern) {
        return TopicConfigurations.of(
                ItemTemplateConfigs.empty(),
                List.of(
                        TopicMappingConfig.fromDelimitedMappings("topic", "item"),
                        TopicMappingConfig.fromDelimitedMappings("topic2", "item")),
                enableSubscriptionPattern);
    }

    KafkaConsumerWrapper<String, String> makeWrapper(
            Set<String> availableTopics, boolean trowExceptionWhileCheckingExistingTopic) {
        return makeWrapper(
                availableTopics,
                trowExceptionWhileCheckingExistingTopic,
                false,
                CommandModeStrategy.NONE,
                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                2,
                RecordConsumeWithOrderStrategy.UNORDERED,
                false);
    }

    KafkaConsumerWrapper<String, String> makeWrapper(
            Set<String> availableTopics,
            boolean trowExceptionWhileCheckingExistingTopic,
            boolean enableSubscriptionPattern,
            CommandModeStrategy commandModeStrategy,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            int threads,
            RecordConsumeWithOrderStrategy orderStrategy,
            boolean allowImplicitItems) {

        // Set the available topics in the mock consumer
        for (String topic : availableTopics) {
            mockConsumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }
        if (trowExceptionWhileCheckingExistingTopic) {
            mockConsumer.setListTopicException(
                    new KafkaException("Fake Exception while checking existing topics"));
        }

        // Create the configuration
        Config<String, String> config =
                makeConfig(
                        enableSubscriptionPattern,
                        commandModeStrategy,
                        errorHandlingStrategy,
                        threads,
                        orderStrategy);

        // Create the SubscribedItems
        SubscribedItems subscribedItems =
                allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create();

        return new KafkaConsumerWrapper<String, String>(
                config, metadataListener, eventListener, subscribedItems, () -> mockConsumer);
    }

    private Config<String, String> makeConfig(
            boolean enableSubscriptionPattern,
            CommandModeStrategy commandModeStrategy,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            int threads,
            RecordConsumeWithOrderStrategy orderStrategy) {
        try {
            return new Config<>(
                    "TestConnection",
                    makeProperties(),
                    Items.templatesFrom(makeTopicsConfig(enableSubscriptionPattern), String()),
                    ItemTemplatesUtils.fieldsExtractor(),
                    OthersSelectorSuppliers.String(),
                    errorHandlingStrategy,
                    commandModeStrategy,
                    new Concurrency(orderStrategy, threads));
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<Arguments> wrapperArgs() {
        return Stream.of(
                Arguments.of(
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        false,
                        false,
                        OrderStrategy.ORDER_BY_PARTITION,
                        ProcessUpdatesType.DEFAULT,
                        false),
                Arguments.of(
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        CommandModeStrategy.ENFORCE,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        false,
                        false,
                        OrderStrategy.ORDER_BY_PARTITION,
                        ProcessUpdatesType.COMMAND,
                        false),
                Arguments.of(
                        2,
                        RecordConsumeWithOrderStrategy.ORDER_BY_KEY,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        false,
                        true,
                        OrderStrategy.ORDER_BY_KEY,
                        ProcessUpdatesType.DEFAULT,
                        false),
                Arguments.of(
                        -1,
                        RecordConsumeWithOrderStrategy.UNORDERED,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        false,
                        true,
                        OrderStrategy.UNORDERED,
                        ProcessUpdatesType.DEFAULT,
                        false),
                Arguments.of(
                        -1,
                        RecordConsumeWithOrderStrategy.UNORDERED,
                        CommandModeStrategy.AUTO,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        true,
                        OrderStrategy.UNORDERED,
                        ProcessUpdatesType.AUTO_COMMAND_MODE,
                        true));
    }

    @ParameterizedTest
    @MethodSource("wrapperArgs")
    public void shouldCreateWrapper(
            int threads,
            RecordConsumeWithOrderStrategy consumedWithOrderStrategy,
            CommandModeStrategy commandModeStrategy,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            boolean allowImplicitItems,
            boolean expectedParallelism,
            OrderStrategy expectedOrderStrategy,
            ProcessUpdatesType expectedProcessUpdatesType,
            boolean expectedRouteImplicitItems) {
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(
                        Collections.emptySet(),
                        false,
                        false,
                        commandModeStrategy,
                        errorHandlingStrategy,
                        threads,
                        consumedWithOrderStrategy,
                        allowImplicitItems);

        assertThat(wrapper.getInternalConsumer()).isSameInstanceAs(mockConsumer);

        // Check the RecordConsumer
        RecordConsumer<String, String> recordConsumer = wrapper.getRecordConsumer();
        if (threads == -1) {
            assertThat(recordConsumer.numOfThreads()).isGreaterThan(1);
        } else {
            assertThat(recordConsumer.numOfThreads()).isEqualTo(threads);
        }
        Optional<OrderStrategy> orderStrategy = recordConsumer.orderStrategy();
        if (threads > 1 || threads == -1) {
            assertThat(orderStrategy).hasValue(expectedOrderStrategy);
        } else {
            assertThat(orderStrategy).isEmpty();
        }
        assertThat(recordConsumer.isParallel()).isEqualTo(expectedParallelism);
        assertThat(recordConsumer.errorStrategy()).isEqualTo(errorHandlingStrategy);

        // Check the RecordProcessor
        RecordProcessor<String, String> recordProcessor = recordConsumer.recordProcessor();
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(expectedProcessUpdatesType);
        assertThat(recordProcessor.canRouteImplicitItems()).isEqualTo(expectedRouteImplicitItems);

        // Check the OffsetService
        OffsetService offsetService = wrapper.getOffsetService();
        assertThat(offsetService.canManageHoles()).isEqualTo(expectedParallelism);
        assertThat(wrapper.getOffsetService().offsetStore()).isPresent();
    }

    @Test
    public void shouldSubscribeToPattern() {
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(
                        Collections.emptySet(),
                        false,
                        true,
                        CommandModeStrategy.NONE,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        false);
        assertThat(wrapper.subscribed()).isTrue();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldInitStoreAndConsume() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);
        mockConsumer.assign(Set.of(partition0, partition1));
        mockConsumer.commitSync(
                Map.of(
                        partition0,
                        new OffsetAndMetadata(0L, "2"),
                        partition1,
                        new OffsetAndMetadata(0L, "1")));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"), 2);

        // Invoke the method and verify that the task has been actually invoked with the
        // expected simulated records
        int filtered = wrapper.initStoreAndConsume(KafkaRecords.from(records));
        assertThat(filtered).isEqualTo(records.count() - 2);
    }

    private void updateOffsets(HashMap<TopicPartition, Long> offsets) {
        if (resetStrategy.equals(EARLIEST)) {
            mockConsumer.updateBeginningOffsets(offsets);
        } else {
            mockConsumer.updateEndOffsets(offsets);
        }
    }

    @Test
    public void shouldPollOnce() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        // Subsequent poll will return the expected records.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<KafkaRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        for (KafkaRecord<String, String> record : received) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        wrapper.pollOnce(task);
        // Received only the scheduled records
        assertThat(holder).hasSize(records.count());
        // Invoked two times: one for the records and one for the empty poll
        assertThat(invocationCounter.get()).isEqualTo(1);
        // Regular interruption
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldPollAllAvailableRecords() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        // Subsequent poll will return the expected records.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<KafkaRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        for (KafkaRecord<String, String> record : received) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        wrapper.pollAvailable(task);
        // Received only the scheduled records
        assertThat(holder).hasSize(records.count());
        // Invoked two times: one for the records and one for the empty poll
        assertThat(invocationCounter.get()).isEqualTo(2);
        // Regular interruption
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldNotPollOnceDueToKafkaException() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));

        // Must subscribe
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Set the KafkaException to be thrown at first poll invocation inside pollForEver
        mockConsumer.setPollException(new KafkaException("Fake Exception"));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<KafkaRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        for (KafkaRecord<String, String> record : received) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        assertThrows(KafkaException.class, () -> wrapper.pollAvailable(task));
        // Never invoked
        assertThat(invocationCounter.get()).isEqualTo(0);
        // Forced interruption
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldPollForEver() throws Exception {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();
        // The third poll will trigger a WakeupException
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Must subscribe
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        // Subsequent poll will return the expected records.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<KafkaRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        for (KafkaRecord<String, String> record : received) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        AtomicBoolean wokenUp =
                new AtomicBoolean(false); // Signals that a WakeupException has been thrown
        CompletableFuture<Void> completable =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                wrapper.pollForEver(task);
                            } catch (WakeupException e) {
                                wokenUp.set(true);
                            }
                        });
        completable.join();
        // A WakeupException has been thrown
        assertThat(wokenUp.get()).isTrue();
        // Received only the scheduled records
        assertThat(holder).hasSize(records.count());
        // Invoked more than once.
        assertThat(invocationCounter.get()).isGreaterThan(1);
        // Regular interruption
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldNotPollForEverDueToKafkaException() throws Exception {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));

        // Must subscribe
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Set the KafkaException to be thrown at first poll invocation inside pollForEver
        mockConsumer.setPollException(new KafkaException("Fake Exception"));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<KafkaRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        for (KafkaRecord<String, String> record : received) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        AtomicBoolean wokenUp =
                new AtomicBoolean(false); // Signals that a WakeupException has been thrown
        AtomicBoolean triggeredException = new AtomicBoolean();
        CompletableFuture<Void> completable =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                wrapper.pollForEver(task);
                            } catch (WakeupException e) {
                                wokenUp.set(true);
                            } catch (KafkaException ke) {
                                triggeredException.set(true);
                            }
                        });
        completable.join();
        // A KafkaException has been triggered
        assertThat(triggeredException.get()).isTrue();
        // A WakeupException has NOT been thrown
        assertThat(wokenUp.get()).isFalse();
        // Never invoked
        assertThat(invocationCounter.get()).isEqualTo(0);
        // Forced interruption
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldStartLoopAndShutdown(boolean forceCommitException)
            throws InterruptedException {
        if (forceCommitException) {
            // Set the exception to be thrown when committing offsets
            mockConsumer.setCommitException(new KafkaException("Fake Exception"));
        }

        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        FutureStatus awaitClose = wrapper.startLoop(Executors.newSingleThreadExecutor(), false);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        TimeUnit.SECONDS.sleep(1);
        assertThat(awaitClose.isStateAvailable()).isFalse();

        // Shutdown the wrapper to make the internal consumer wakeup and exit the loop
        FutureStatus finalStatus = wrapper.shutdown();
        assertThat(awaitClose.isStateAvailable()).isTrue();
        assertThat(awaitClose.join()).isEqualTo(State.LOOP_CLOSED_BY_WAKEUP);
        assertThat(finalStatus.isShutdown());

        assertThat(mockConsumer.closed());
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
        OffsetStore offsetStore = wrapper.getOffsetService().offsetStore().get();
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.current();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @Test
    public void shouldChangeStatusConsistently() throws InterruptedException {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        FutureStatus awaitClose = wrapper.startLoop(Executors.newSingleThreadExecutor(), false);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        TimeUnit.SECONDS.sleep(1);

        // Try to start the loop again and verify that the status has not changed
        FutureStatus awaitClose2 = wrapper.startLoop(Executors.newSingleThreadExecutor(), false);
        assertThat(awaitClose2).isSameInstanceAs(awaitClose);

        // Shutdown the wrapper to make the internal consumer wakeup and exit the loop
        FutureStatus finalStatus = wrapper.shutdown();
        assertThat(awaitClose2.join()).isEqualTo(State.LOOP_CLOSED_BY_WAKEUP);
        assertThat(finalStatus.isShutdown());

        // Try to start the loop again and verify that the status is stull SHUTDOWN
        FutureStatus currentStatus = wrapper.startLoop(Executors.newSingleThreadExecutor(), false);
        assertThat(currentStatus).isSameInstanceAs(finalStatus);

        assertThat(wrapper.shutdown()).isSameInstanceAs(finalStatus);
    }

    @Test
    public void shouldShutdownEvenWithoutStartingLoop() throws InterruptedException {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Shutdown the wrapper to make the internal consumer wakeup and exit the loop
        FutureStatus status = wrapper.shutdown();
        assertThat(status.isStateAvailable()).isTrue();
        assertThat(status.join()).isEqualTo(State.SHUTDOWN);
        assertThat(mockConsumer.closed());
        // The RecordConsumer is not terminated because the loop was never started
        assertThat(wrapper.getRecordConsumer().isTerminated()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartLoopDueToNotExistingTopic(boolean waitForInit) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        // Create a wrapper for a non-existing topic
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("anotherTopic"), false);

        FutureStatus status = wrapper.startLoop(executorService, waitForInit);
        if (waitForInit) {
            // If waitForInit is true, the status is immediately set to INIT_FAILED_BY_SUBSCRIPTION
            assertThat(status.initFailed()).isTrue();
        }
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_BY_SUBSCRIPTION);
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartLoopDueToExceptionWhileCheckingExistingTopic(boolean waitForInit) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        // Create a wrapper for a topic that exists but cannot be subscribed due to an exception
        // throw
        // while checking the topic's existence
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("topic"), true);

        FutureStatus status = wrapper.startLoop(executorService, waitForInit);
        if (waitForInit) {
            // If waitForInit is true, the status is immediately set to INIT_FAILED_BY_SUBSCRIPTION
            assertThat(status.initFailed()).isTrue();
        }
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_BY_SUBSCRIPTION);
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartLoopDueToPollExceptionWhileInitPolling(boolean waitForInit) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("topic"), false);

        mockConsumer.setPollException(new KafkaException("Fake Exception"));
        FutureStatus status = wrapper.startLoop(executorService, waitForInit);
        if (waitForInit) {
            // If waitForInit is true, the status is immediately set to INIT_FAILED_BY_EXCEPTION
            assertThat(status.initFailed()).isTrue();
        }
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_BY_EXCEPTION);
        // Verify that the consumer has subscribed
        assertThat(mockConsumer.subscription()).containsExactly("topic");
        // But everything else is in the expected state as per the exception
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartLoopDueToOffsetExceptionWhileInitPolling(boolean waitForInit) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("topic"), false);

        mockConsumer.setOffsetsException(new KafkaException("Fake Exception"));
        FutureStatus status = wrapper.startLoop(executorService, waitForInit);
        if (waitForInit) {
            // If waitForInit is true, the status is immediately set to INIT_FAILED_BY_EXCEPTION
            assertThat(status.initFailed()).isTrue();
        }
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_BY_EXCEPTION);
        // Verify that the consumer has subscribed
        assertThat(mockConsumer.subscription()).containsExactly("topic");
        // But everything else is in the expected state as per the exception
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();
    }

    @Test
    public void shouldInterruptLoopDueToKafkaException() throws Exception {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));
        mockConsumer.setOffsetsException(null);

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<Deferred<String>, Deferred<String>> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        FutureStatus awaitClose = wrapper.startLoop(Executors.newSingleThreadExecutor(), false);
        TimeUnit.SECONDS.sleep(1);
        assertThat(awaitClose.isStateAvailable()).isFalse();

        // Set the KafkaException to be thrown at next poll invocation inside pollForEver
        mockConsumer.setPollException(new KafkaException("Fake Exception"));

        // Verify that the loop has been interrupted by the simulated exception
        assertThat(awaitClose.join()).isEqualTo(State.LOOP_CLOSED_BY_EXCEPTION);
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(mockConsumer.closed());
        assertThat(wrapper.getRecordConsumer().isTerminated()).isTrue();

        FutureStatus shutdown = wrapper.shutdown();
        assertThat(shutdown.isStateAvailable()).isTrue();
        assertThat(shutdown.join()).isEqualTo(State.SHUTDOWN);
    }
}
