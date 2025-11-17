
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

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper.AdminInterface;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ConsumerWrapperImplTest {

    private MockMetadataListener metadataListener;
    private MockItemEventListener itemEventListener;
    private final SubscribedItems subscribedItems = SubscribedItems.of(Collections.emptyList());
    private MockConsumer<String, String> mockConsumer;
    private final OffsetResetStrategy resetStrategy = OffsetResetStrategy.EARLIEST;

    @BeforeEach
    public void setUp() {
        metadataListener = new Mocks.MockMetadataListener();
        itemEventListener = new Mocks.MockItemEventListener();
        mockConsumer = new MockConsumer<>(resetStrategy);
    }

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

    ConsumerWrapperImpl<String, String> mkConsumerWrapper(AdminInterface admin) {
        return mkConsumerWrapper(
                admin,
                false,
                false,
                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                2,
                RecordConsumeWithOrderStrategy.UNORDERED);
    }

    ConsumerWrapperImpl<String, String> mkConsumerWrapper(
            AdminInterface admin,
            boolean enableSubscriptionPattern,
            boolean enforceCommandMode,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            int threads,
            RecordConsumeWithOrderStrategy orderStrategy) {
        return (ConsumerWrapperImpl<String, String>)
                ConsumerWrapper.create(
                        new Mocks.MockTriggerConfig(
                                makeTopicsConfig(enableSubscriptionPattern),
                                makeProperties(),
                                enforceCommandMode,
                                errorHandlingStrategy,
                                threads,
                                orderStrategy),
                        itemEventListener,
                        metadataListener,
                        subscribedItems,
                        () -> mockConsumer,
                        p -> admin);
    }

    static Stream<Arguments> wrapperArgs() {
        return Stream.of(
                Arguments.of(
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        false,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        false,
                        OrderStrategy.ORDER_BY_PARTITION),
                Arguments.of(
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        true,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        false,
                        OrderStrategy.ORDER_BY_PARTITION),
                Arguments.of(
                        2,
                        RecordConsumeWithOrderStrategy.ORDER_BY_KEY,
                        false,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        OrderStrategy.ORDER_BY_KEY),
                Arguments.of(
                        -1,
                        RecordConsumeWithOrderStrategy.UNORDERED,
                        false,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        OrderStrategy.UNORDERED));
    }

    @ParameterizedTest
    @MethodSource("wrapperArgs")
    public void shouldCreateConsumerWrapper(
            int threads,
            RecordConsumeWithOrderStrategy consumedWithOrderStrategy,
            boolean enforceCommandMode,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            boolean expectedParallelism,
            OrderStrategy expectedOrderStrategy) {
        ConsumerWrapperImpl<String, String> consumerWrapper =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.emptySet()),
                        false,
                        enforceCommandMode,
                        errorHandlingStrategy,
                        threads,
                        consumedWithOrderStrategy);

        assertThat(consumerWrapper.getConsumer()).isSameInstanceAs(mockConsumer);

        // Check the RecordConsumer
        RecordConsumer<String, String> recordConsumer = consumerWrapper.getRecordConsumer();
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
        assertThat(recordConsumer.isCommandEnforceEnabled()).isEqualTo(enforceCommandMode);
        assertThat(recordConsumer.errorStrategy()).isEqualTo(errorHandlingStrategy);

        // Check the OffsetService
        OffsetService offsetService = consumerWrapper.getOffsetService();
        assertThat(offsetService.canManageHoles()).isEqualTo(expectedParallelism);
        assertThat(consumerWrapper.getOffsetService().offsetStore()).isPresent();

        // Check the poll timeout
        assertThat(consumerWrapper.getPollTimeout())
                .isEqualTo(ConsumerWrapperImpl.MAX_POLL_DURATION);
    }

    @Test
    public void shouldNotSubscribeDueToNotExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.singleton("anotherTopic")));
        assertThat(consumer.subscribed()).isFalse();
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldNotSubscribeDueToExceptionWhileCheckingExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.singleton("topic"), true));
        assertThat(consumer.subscribed()).isFalse();
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldSubscribeToTopic() {
        String topic = "topic";
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));
        assertThat(consumer.subscribed()).isTrue();
        assertThat(mockConsumer.subscription()).containsExactly(topic);
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldSubscribeToPattern() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.emptySet()),
                        true,
                        false,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION);
        assertThat(consumer.subscribed()).isTrue();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldInitAndStore() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

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
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 10, List.of("a", "b"), 2);

        // Invoke the method and verify that the task has been actually invoked with the
        // expected simulated records
        ConsumerRecords<String, String> filtered = consumer.initStoreAndConsume(records);
        assertThat(filtered.count()).isEqualTo(records.count() - 2);
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
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        // Subsequent poll will return the expected records.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Define the task that should be invoked upon polling
        List<ConsumerRecords<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<ConsumerRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        holder.add(received);
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        consumer.pollOnce(task);
        assertThat(holder.get(0).count()).isEqualTo(records.count());
        // Received only the scheduled records
        assertThat(holder).hasSize(1);
        // Invoked only once
        assertThat(invocationCounter.get()).isEqualTo(1);
        // Regular interruption
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldNotPollOnceDueToKafkaException() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        updateOffsets(offsets);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
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
        List<ConsumerRecords<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<ConsumerRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        holder.add(received);
                    }
                };

        // Invoke the method and verify that task has been actually
        // invoked with the expected simulated records.
        assertThrows(KafkaException.class, () -> consumer.pollOnce(task));
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

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
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
        List<ConsumerRecords<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<ConsumerRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        holder.add(received);
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
                                consumer.pollForEver(task);
                            } catch (WakeupException e) {
                                wokenUp.set(true);
                            }
                        });
        completable.join();
        // A WakeupException has been thrown
        assertThat(wokenUp.get()).isTrue();
        // Received only the scheduled records
        assertThat(holder).hasSize(1);
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

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
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
        List<ConsumerRecords<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<ConsumerRecords<String, String>> task =
                received -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (received.count() > 0) {
                        holder.add(received);
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
                                consumer.pollForEver(task);
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

    @Test
    public void shouldRun() {
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

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();
        // The third poll will trigger a WakeupException so that the infinite loop can be broken
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        consumer.run();

        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
        OffsetStore offsetStore = consumer.getOffsetService().offsetStore().get();
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.snapshot();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @Test
    public void shouldRunAndCloseDueToFailure() throws InterruptedException, ExecutionException {
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

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        CompletableFuture<Void> completable = CompletableFuture.runAsync(() -> consumer.run());
        TimeUnit.SECONDS.sleep(1);

        // Trigger a KafkaException
        mockConsumer.setPollException(new KafkaException("Fake Exception"));
        CompletionException ce = assertThrows(CompletionException.class, () -> completable.join());
        assertThat(ce).hasCauseThat().isInstanceOf(KafkaException.class);

        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        OffsetStore offsetStore = consumer.getOffsetService().offsetStore().get();
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.snapshot();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldClose(boolean forceCommitException) throws InterruptedException {
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

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(Collections.singleton(topic)));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        CompletableFuture<Void> completable = CompletableFuture.runAsync(() -> consumer.run());
        TimeUnit.SECONDS.sleep(1);

        // Wake up it
        consumer.close();
        completable.join();

        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
        OffsetStore offsetStore = consumer.getOffsetService().offsetStore().get();
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.snapshot();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @Test
    public void shouldRunWithNoSubscriptionDueToNotExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.singleton("anotherTopic")));

        consumer.run();

        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldRunWithNoSubscriptionDueToExceptionWhileCheckingExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(
                        new Mocks.MockAdminInterface(Collections.singleton("anotherTopic"), true));

        consumer.run();

        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }
}
