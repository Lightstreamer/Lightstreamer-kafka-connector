
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
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.SubscriptionOutcome.PATTERN;
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.SubscriptionOutcome.TOPICS;
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ConsumerGroupMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.SubscriptionOutcome;
import com.lightstreamer.kafka.adapters.consumers.RecordDeserializationMode.DeserializationTiming;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ForceableSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.RecordBatch;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks.EventCall;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class KafkaConsumerWrapperTest {

    private final StrategyType resetStrategy = StrategyType.EARLIEST;
    private static final Logger logger = LogFactory.getLogger("TestConnection");

    private final AtomicReference<Throwable> loopFailureCause = new AtomicReference<>();
    private final java.util.function.Consumer<Throwable> onLoopClosedByExceptionAction =
            cause -> {
                loopFailureCause.set(cause);
            };

    private MockItemEventListener itemEventListener = new MockItemEventListener();

    private MockConsumer mockConsumer = new MockConsumer(resetStrategy.toString());

    private KafkaRecord.DeserializerPair<String, String> deserializerPair =
            new KafkaRecord.DeserializerPair<>(
                    OthersSelectorSuppliers.String().keySelectorSupplier().deserializer(),
                    OthersSelectorSuppliers.String().valueSelectorSupplier().deserializer());

    private SubscribedItems subscribedItems;

    private Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                resetStrategy.equals(StrategyType.EARLIEST) ? "earliest" : "latest");
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
            Set<String> topicsBroker,
            boolean trowExceptionWhileCheckingExistingTopic,
            boolean eagerLifecycle) {
        return makeWrapper(
                topicsBroker,
                trowExceptionWhileCheckingExistingTopic,
                false,
                false,
                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                2,
                RecordConsumeWithOrderStrategy.UNORDERED,
                eagerLifecycle);
    }

    KafkaConsumerWrapper<String, String> makeWrapper(
            Set<String> topicsBroker,
            boolean trowExceptionWhileCheckingExistingTopic,
            boolean enableSubscriptionPattern,
            boolean processAsCommand,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            int threads,
            RecordConsumeWithOrderStrategy orderStrategy,
            boolean eagerLifecycle) {

        // Set the topics in the mock consumer
        for (String topic : topicsBroker) {
            this.mockConsumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }
        if (trowExceptionWhileCheckingExistingTopic) {
            this.mockConsumer.setListTopicException(
                    new KafkaException("Fake Exception while checking existing topics"));
        }

        // Create the configuration
        ConnectionSpec<String, String> spec =
                makeConnectionSpec(
                        enableSubscriptionPattern,
                        processAsCommand,
                        errorHandlingStrategy,
                        threads,
                        orderStrategy);

        // Create the SubscribedItems
        this.subscribedItems =
                eagerLifecycle
                        ? SubscribedItems.forceable(itemEventListener, logger)
                        : SubscribedItems.onDemand();

        KafkaConsumerWrapper<String, String> wrapper =
                new KafkaConsumerWrapper<String, String>(
                        spec,
                        itemEventListener,
                        subscribedItems,
                        prop -> this.mockConsumer,
                        eagerLifecycle);
        return wrapper;
    }

    private ConnectionSpec<String, String> makeConnectionSpec(
            boolean enableSubscriptionPattern,
            boolean processAsCommand,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            int threads,
            RecordConsumeWithOrderStrategy orderStrategy) {
        try {
            return new ConnectionSpec<>(
                    "TestConnection",
                    makeProperties(),
                    Items.templatesFrom(makeTopicsConfig(enableSubscriptionPattern), String()),
                    ItemTemplatesUtils.fieldsExtractor(),
                    deserializerPair,
                    errorHandlingStrategy,
                    processAsCommand,
                    new Concurrency(orderStrategy, threads),
                    ConsumerGroupMode.GROUP);
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<Arguments> wrapperArgs() {
        return Stream.of(
                Arguments.of(
                        // threads
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        false,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        // expectedParallelism
                        false,
                        OrderStrategy.ORDER_BY_PARTITION,
                        ProcessUpdatesType.DEFAULT,
                        // eagerLifecycle
                        false),
                Arguments.of(
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        true,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        false,
                        OrderStrategy.ORDER_BY_PARTITION,
                        ProcessUpdatesType.COMMAND_MODE,
                        false),
                Arguments.of(
                        2,
                        RecordConsumeWithOrderStrategy.ORDER_BY_KEY,
                        false,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        OrderStrategy.ORDER_BY_KEY,
                        ProcessUpdatesType.DEFAULT,
                        true),
                Arguments.of(
                        -1,
                        RecordConsumeWithOrderStrategy.UNORDERED,
                        false,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        OrderStrategy.UNORDERED,
                        ProcessUpdatesType.DEFAULT,
                        false),
                Arguments.of(
                        -1,
                        RecordConsumeWithOrderStrategy.UNORDERED,
                        true,
                        RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION,
                        true,
                        OrderStrategy.UNORDERED,
                        ProcessUpdatesType.COMMAND_MODE,
                        false));
    }

    @ParameterizedTest
    @MethodSource("wrapperArgs")
    public void shouldCreateWrapper(
            int threads,
            RecordConsumeWithOrderStrategy consumedWithOrderStrategy,
            boolean processAsCommand,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            boolean expectedParallelism,
            OrderStrategy expectedOrderStrategy,
            ProcessUpdatesType expectedProcessUpdatesType,
            boolean eagerLifecycle) {
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(
                        Collections.emptySet(),
                        false,
                        false,
                        processAsCommand,
                        errorHandlingStrategy,
                        threads,
                        consumedWithOrderStrategy,
                        eagerLifecycle);

        assertThat(wrapper.getInternalConsumer()).isSameInstanceAs(mockConsumer);

        // Check the RecordConsumer
        RecordConsumer<String, String> recordConsumer = wrapper.getRecordConsumer();
        if (threads == -1) {
            assertThat(recordConsumer.numOfThreads()).isGreaterThan(1);
        } else {
            assertThat(recordConsumer.numOfThreads()).isEqualTo(threads);
        }
        assertThat(recordConsumer.isParallel()).isEqualTo(expectedParallelism);

        // Check the OrderStrategy
        Optional<OrderStrategy> orderStrategy = recordConsumer.ordering();
        if (threads > 1 || threads == -1) {
            assertThat(orderStrategy).hasValue(expectedOrderStrategy);
        } else {
            assertThat(orderStrategy).isEmpty();
        }

        // Check the ErrorHandlingStrategy
        assertThat(recordConsumer.errorStrategy()).isEqualTo(errorHandlingStrategy);

        // Check the RecordProcessor
        RecordProcessor<String, String> recordProcessor = recordConsumer.recordProcessor();
        assertThat(recordProcessor.processUpdatesType()).isEqualTo(expectedProcessUpdatesType);

        // Check the DeserializationTiming
        DeserializationTiming recordDeserializationTiming =
                wrapper.getRecordDeserializationTiming();
        assertThat(recordDeserializationTiming).isEqualTo(DeserializationTiming.EAGER);

        // Check the OffsetService
        String simpleName = wrapper.getOffsetService().getClass().getSimpleName();
        if (eagerLifecycle) {
            assertThat(simpleName).isEqualTo("SeekingOffsetService");
        } else {
            assertThat(simpleName).isEqualTo("CommitOffsetService");
        }

        // Check the poll timeout
        assertThat(wrapper.getPollTimeout()).isEqualTo(KafkaConsumerWrapper.MAX_POLL_DURATION);
    }

    static Stream<Arguments> subscriptionFlags() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false));
    }

    @ParameterizedTest
    @MethodSource("subscriptionFlags")
    public void shouldSubscribeToTopicsOrPattern(
            boolean eagerLifecycle, boolean enableSubscriptionToPattern) {
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(
                        Set.of("topic", "topic2"),
                        false,
                        enableSubscriptionToPattern,
                        false,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        eagerLifecycle);
        if (enableSubscriptionToPattern) {
            assertThat(wrapper.trySubscribe()).isEqualTo(PATTERN);
        } else {
            assertThat(wrapper.trySubscribe()).isEqualTo(TOPICS);
        }
        assertThat(mockConsumer.listTopics()).hasSize(2);
    }

    static Stream<Arguments> subscriptionArgs() {
        return Stream.of(
                // All requested topics are available. Should subscribe successfully.
                Arguments.of(Set.of("topic", "topic2"), SubscriptionOutcome.TOPICS),
                // Available topics are a subset of the requested ones. Should subscribe to the
                // available ones and log a warning.
                Arguments.of(Set.of("topic"), SubscriptionOutcome.TOPICS),
                // No requested topic is available. Should not subscribe and log a warning.
                Arguments.of(Set.of(), SubscriptionOutcome.NONE),
                // No requested topic is available. Should not subscribe and log a warning.
                Arguments.of(Set.of("nonExistingTopic"), SubscriptionOutcome.NONE));
    }

    @ParameterizedTest
    @MethodSource("subscriptionArgs")
    public void shouldTrySubscribeReturnExpectedOutcome(
            Set<String> availableTopicsOnBroker, SubscriptionOutcome expectedOutcome) {
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(
                        availableTopicsOnBroker,
                        false,
                        false,
                        false,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        1,
                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION,
                        false);
        assertThat(wrapper.trySubscribe()).isEqualTo(expectedOutcome);
    }

    @Test
    public void shouldCompleteCatchUpImmediatelyWhenTopicIsEmpty() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Begin offsets equal end offsets: both partitions are already at their end, so catch-up
        // completes on the very first poll without consuming any record.
        updateBeginAndEndOffsets(
                Map.of(partition0, 0L, partition1, 0L), Map.of(partition0, 0L, partition1, 0L));

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false, true);

        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(status.isStateAvailable()).isFalse();

        // No records were consumed, so no forced item is created and no event is delivered.
        assertThat(subscribedItems.values()).isEmpty();
        assertThat(itemEventListener.getEvents()).isEmpty();
    }

    @Test
    public void shouldCompleteCatchUpInSinglePollWhenAllPartitionsReachEndOffsets() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // partition0 ends at offset 2, partition1 ends at offset 2: a single record-bearing poll
        // brings both partitions to their end, so catch-up completes without polling again.
        updateBeginAndEndOffsets(
                Map.of(partition0, 0L, partition1, 0L), Map.of(partition0, 2L, partition1, 2L));

        // Counts the record-bearing polls actually performed by the catch-up loop.
        AtomicInteger recordPolls = new AtomicInteger();

        // A single poll fills both partitions to their end (offsets 0,1 each).
        mockConsumer.schedulePollTask(
                () -> {
                    recordPolls.incrementAndGet();
                    mockConsumer.addRecord(record(topic, 0, 0));
                    mockConsumer.addRecord(record(topic, 0, 1));
                    mockConsumer.addRecord(record(topic, 1, 0));
                    mockConsumer.addRecord(record(topic, 1, 1));
                });

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false, true);
        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(status.isStateAvailable()).isFalse();

        // Catch-up completed after a single record-bearing poll.
        assertThat(recordPolls.get()).isEqualTo(1);

        // Exactly one forced item is created while consuming the snapshot records.
        assertThat(subscribedItems.values()).hasSize(1);
        ForceableSubscribedItem item =
                (ForceableSubscribedItem) subscribedItems.values().iterator().next();

        // Events are buffered during catch-up: enabling delivery flushes them in order.
        Object handle = new Object();
        item.enableEventsDelivery(handle, itemEventListener);

        // Verify that all events are flagged as no snapshot, as we let the server handle internal
        // snapshot state.
        List<EventCall> events = itemEventListener.getEvents();
        assertThat(events).hasSize(4);
        assertThat(events.stream().allMatch(EventCall::isSnapshot)).isFalse();
    }

    @Test
    public void shouldNotCompleteCatchUpUntilAllPartitionsReachEndOffsets() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // partition0 ends at offset 2, partition1 ends at offset 4: catch-up must keep polling
        // until BOTH partitions have been fully consumed.
        updateBeginAndEndOffsets(
                Map.of(partition0, 0L, partition1, 0L), Map.of(partition0, 2L, partition1, 4L));

        // Counts the record-bearing polls actually performed by the catch-up loop. Each scheduled
        // task runs on a distinct poll() invocation, so the counter directly measures how many
        // polls were needed to drain both partitions.
        AtomicInteger recordPolls = new AtomicInteger();

        // First poll brings partition0 to its end (offsets 0,1) but partition1 only halfway
        // (offsets 0,1): catch-up must NOT complete yet.
        mockConsumer.schedulePollTask(
                () -> {
                    recordPolls.incrementAndGet();
                    mockConsumer.addRecord(record(topic, 0, 0));
                    mockConsumer.addRecord(record(topic, 0, 1));
                    mockConsumer.addRecord(record(topic, 1, 0));
                    mockConsumer.addRecord(record(topic, 1, 1));
                });

        // Second poll completes partition1 (offsets 2,3): only now does catch-up finish.
        mockConsumer.schedulePollTask(
                () -> {
                    recordPolls.incrementAndGet();
                    mockConsumer.addRecord(record(topic, 1, 2));
                    mockConsumer.addRecord(record(topic, 1, 3));
                });

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false, true);

        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(status.isStateAvailable()).isFalse();

        // Catch-up did not stop after partition0 reached its end: it polled a second time to
        // finish partition1.
        assertThat(recordPolls.get()).isEqualTo(2);

        assertThat(subscribedItems.values()).hasSize(1);
        ForceableSubscribedItem item =
                (ForceableSubscribedItem) subscribedItems.values().iterator().next();

        Object handle = new Object();
        item.enableEventsDelivery(handle, itemEventListener);

        // Verify that all events are flagged as no snapshot, as we let the server handle internal
        // snapshot state.
        List<EventCall> events = itemEventListener.getEvents();
        assertThat(events).hasSize(6);
        assertThat(events.stream().allMatch(EventCall::isSnapshot)).isFalse();
    }

    @Test
    public void shouldDeliverRealTimeUpdatesAfterCatchUp() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method.
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Both partitions end at offset 10: catch-up must consume offsets 0..9 on both partitions
        // before completing.
        updateBeginAndEndOffsets(
                Map.of(partition0, 0L, partition1, 0L), Map.of(partition0, 10L, partition1, 10L));

        // 22 records (offsets 0..10 per partition) complete catch-up.
        ConsumerRecords<byte[], byte[]> snapshotRecords =
                Records.generateRecords(topic, 22, List.of("a", "b"), 2);
        mockConsumer.schedulePollTask(
                () -> snapshotRecords.forEach(record -> mockConsumer.addRecord(record)));

        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton(topic), false, true);

        // After catch-up, the consume loop processes two records arriving just past the end
        // offsets (offset 11 per partition), then a scheduled shutdown terminates the loop
        // cleanly — making the whole test deterministic via status.join().
        mockConsumer.schedulePollTask(
                () -> {
                    mockConsumer.addRecord(record(topic, 0, 11));
                    mockConsumer.addRecord(record(topic, 1, 11));
                });
        mockConsumer.schedulePollTask(() -> CompletableFuture.runAsync(wrapper::shutdown));

        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);

        // Waiting for the shutdown-driven exit guarantees that both the catch-up snapshot records
        // and the post-catch-up real-time records have already been processed by the loop.
        assertThat(status.join()).isEqualTo(State.LOOP_CLOSED_ON_WAKEUP);

        // Flushing the buffered events delivers everything in arrival order, with the snapshot
        // records first,
        // then the real-time records. There is no snapshot flag on any event (nor an explicit
        // end-of-snapshot event),
        //  as we let the server handle internal snapshot state.
        ForceableSubscribedItem item =
                (ForceableSubscribedItem) subscribedItems.values().iterator().next();
        item.enableEventsDelivery(new Object(), itemEventListener);

        List<EventCall> events = itemEventListener.getEvents();
        assertThat(events).hasSize(24);
        assertThat(events.stream().allMatch(EventCall::isSnapshot)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartDueToNotExistingTopic(boolean eagerLifecycle) {
        // Create a wrapper for a topic that doesn't exist on the broker
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("anotherTopic"), false, eagerLifecycle);
        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);

        // The status is immediately set to INIT_FAILED_ON_MISSING_TOPICS
        assertThat(status.initFailed()).isTrue();
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_ON_MISSING_TOPICS);
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        FutureStatus shutdown = wrapper.shutdown();
        assertThat(shutdown.join()).isEqualTo(State.SHUTDOWN);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotStartDueToExceptionWhileCheckingExistingTopic(boolean eagerLifecycle) {
        // Create a wrapper for a topic that exists in the broker but cannot be subscribed due to
        // an exception thrown while checking the topic's existence
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("topic"), true, eagerLifecycle);
        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);

        // The status is immediately set to INIT_FAILED_ON_ERROR
        assertThat(status.initFailed()).isTrue();
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_ON_ERROR);
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        assertThat(wrapper.shutdown().join()).isEqualTo(State.SHUTDOWN);
    }

    @Test
    public void shouldNotStartDueToExceptionWhileCatchingUp() {
        // Schedule an exception to be thrown when fetching offsets during catch-up.
        // The wrapper should fail to start, closing the consumer and not starting the monitor.
        mockConsumer.schedulePollTask(
                () -> {
                    throw new KafkaException("Fake Exception while fetching offsets");
                });

        // Create a wrapper for a topic that exists in the broker
        KafkaConsumerWrapper<String, String> wrapper =
                makeWrapper(Collections.singleton("topic"), false, true);
        FutureStatus status =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);

        // The status is immediately set to INIT_FAILED_ON_ERROR
        assertThat(status.initFailed()).isTrue();
        assertThat(status.join()).isEqualTo(State.INIT_FAILED_ON_ERROR);
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        assertThat(wrapper.shutdown().join()).isEqualTo(State.SHUTDOWN);
    }

    @Test
    public void shouldConsumeForEver() throws Exception {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> records =
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
        Consumer<RecordBatch<String, String>> task =
                batch -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (batch.count() > 0) {
                        for (KafkaRecord<String, String> record : batch.getRecords()) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually invoked with the expected
        // simulated records.
        AtomicBoolean wokenUp =
                new AtomicBoolean(false); // Signals that a WakeupException has been thrown
        CompletableFuture<Void> completable =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                wrapper.consumeForEver(task);
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
    }

    @Test
    public void shouldNotConsumeForEverDueToKafkaException() throws Exception {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> records =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> records.forEach(record -> mockConsumer.addRecord(record)));

        // Must subscribe
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Set the KafkaException to be thrown at first poll invocation inside consumeForEver
        mockConsumer.setPollException(new KafkaException("Fake Exception"));

        // Define the task that should be invoked upon polling
        List<KafkaRecord<String, String>> holder = new ArrayList<>();
        AtomicInteger invocationCounter = new AtomicInteger();
        Consumer<RecordBatch<String, String>> task =
                batch -> {
                    // Track invocations
                    invocationCounter.incrementAndGet();
                    // Store only if records were actually fetched
                    if (batch.count() > 0) {
                        for (KafkaRecord<String, String> record : batch.getRecords()) {
                            holder.add(record);
                        }
                    }
                };

        // Invoke the method and verify that task has been actually invoked with the expected
        // simulated records.
        AtomicBoolean wokenUp =
                new AtomicBoolean(false); // Signals that a WakeupException has been thrown
        AtomicBoolean triggeredException = new AtomicBoolean();
        CompletableFuture<Void> completable =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                wrapper.consumeForEver(task);
                            } catch (WakeupException e) {
                                wokenUp.set(true);
                            } catch (KafkaException ke) {
                                triggeredException.set(true);
                            }
                        });
        completable.join();
        // A WakeupException has NOT been thrown
        assertThat(wokenUp.get()).isFalse();
        // A KafkaException has been triggered
        assertThat(triggeredException.get()).isTrue();
        // Never invoked
        assertThat(invocationCounter.get()).isEqualTo(0);
    }

    @Test
    public void shouldNotConsumeForEverDueToExceptionWhileConsumingRecords() throws Exception {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> consumerRecords.forEach(record -> mockConsumer.addRecord(record)));

        // Must subscribe
        mockConsumer.subscribe(Set.of(topic));
        // The following poll is only required to trigger the rebalance set above.
        mockConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        // Define the task that should be invoked upon polling
        Consumer<RecordBatch<String, String>> task =
                batch -> {
                    throw new SerializationException("Fake Exception while consuming records");
                };

        AtomicBoolean wokenUp =
                new AtomicBoolean(false); // Signals that a WakeupException has been thrown
        AtomicBoolean triggeredException = new AtomicBoolean();
        CompletableFuture<Void> completable =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                wrapper.consumeForEver(task);
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
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldChangeStatusConsistently(boolean forceCommitException)
            throws InterruptedException {
        if (forceCommitException) {
            // Set the exception to be thrown when committing offsets.
            // The shutdown process should complete anyway, trying to commit offsets only once and
            // logging the exception.
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords(topic, 100, List.of("a", "b"), 2);
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> {
                    consumerRecords.forEach(record -> mockConsumer.addRecord(record));
                });
        // The second poll blocks inside poll() long enough for the shutdown below to set the
        // wakeup flag while the loop is still parked in poll(), forcing the
        // LOOP_CLOSED_BY_WAKEUP path (the loop is woken up rather than observing the closed flag).
        mockConsumer.schedulePollTask(
                () -> {
                    try {
                        logger.atInfo().log("Entering blocking poll task");
                        TimeUnit.MILLISECONDS.sleep(1000);
                        logger.atInfo().log("Exiting blocking poll task");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        // Run the consumer for at least 1 second
        FutureStatus awaitClose =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        TimeUnit.MILLISECONDS.sleep(20);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        assertThat(wrapper.getMonitor().isRunning()).isTrue();

        // Try to start the loop again and verify that the status has not changed
        FutureStatus awaitClose2 =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(awaitClose2).isSameInstanceAs(awaitClose);

        // Shutdown the wrapper to make the internal consumer wakeup and exit the loop
        FutureStatus finalStatus = wrapper.shutdown();
        assertThat(awaitClose2.isStateAvailable()).isTrue();
        assertThat(awaitClose2.join()).isEqualTo(State.LOOP_CLOSED_ON_WAKEUP);
        assertThat(finalStatus.isShutdown()).isTrue();
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(loopFailureCause.get()).isNull();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        // Verify that offsets have been moved reasonably
        Map<TopicPartition, OffsetAndMetadata> map = wrapper.getOffsetService().offsetsSnapshot();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        assertThat(map.get(partition0).offset()).isGreaterThan(consumerRecords.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(consumerRecords.count() / 3);

        // Try to start the loop again and verify that the status is still SHUTDOWN
        FutureStatus currentStatus =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        assertThat(currentStatus).isSameInstanceAs(finalStatus);
        assertThat(wrapper.shutdown()).isSameInstanceAs(finalStatus);
    }

    @Test
    public void shouldShutdownEvenWithoutStarting() throws InterruptedException {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Shutdown the wrapper to make the internal consumer wakeup and exit the loop
        FutureStatus status = wrapper.shutdown();
        assertThat(status.isStateAvailable()).isTrue();
        assertThat(status.join()).isEqualTo(State.SHUTDOWN);

        // Verify that the internal resources have been closed even if the loop was never started
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();
    }

    @Test
    public void shouldInterruptConsumptionAfterStartingDueToKafkaException() throws Exception {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> consumerRecords.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        FutureStatus awaitClose =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        TimeUnit.SECONDS.sleep(1);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        assertThat(wrapper.getMonitor().isRunning()).isTrue();

        // Set the KafkaException to be thrown at next poll invocation inside pollForEver
        mockConsumer.setPollException(new KafkaException("Fake Exception"));

        // Verify that the loop has been interrupted by the simulated exception
        assertThat(awaitClose.join()).isEqualTo(State.LOOP_CLOSED_ON_ERROR);
        Throwable failure = loopFailureCause.get();
        assertThat(failure).isInstanceOf(KafkaException.class);
        assertThat(failure).hasMessageThat().isEqualTo("Fake Exception");
        assertThat(failure).hasCauseThat().isNull();

        // Verify that the internal resources have been closed
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        // Try to shutdown again and verify that the status is still SHUTDOWN
        FutureStatus shutdown = wrapper.shutdown();
        assertThat(shutdown.isStateAvailable()).isTrue();
        assertThat(shutdown.join()).isEqualTo(State.SHUTDOWN);
    }

    @Test
    public void shouldInterruptConsumptionAfterStartingDueToGenericException() throws Exception {
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
                makeWrapper(Collections.singleton(topic), false, false);

        // Generate the simulated records to be polled from the mocked consumer
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords(topic, 10, List.of("a", "b"));
        // The first poll will return the simulated records
        mockConsumer.schedulePollTask(
                () -> consumerRecords.forEach(record -> mockConsumer.addRecord(record)));
        // The second poll will return nothing
        mockConsumer.scheduleNopPollTask();

        // Run the consumer for at least 1 second
        FutureStatus awaitClose =
                wrapper.start(Executors.newSingleThreadExecutor(), onLoopClosedByExceptionAction);
        TimeUnit.SECONDS.sleep(1);
        assertThat(awaitClose.isStateAvailable()).isFalse();
        assertThat(wrapper.getMonitor().isRunning()).isTrue();

        // Set the KafkaException to be thrown at next poll invocation inside pollForEver
        // mockConsumer.setPollException(new KafkaException("Fake Exception"));
        mockConsumer.schedulePollTask(
                () -> {
                    throw new IllegalStateException("Fake Exception");
                });

        // Verify that the loop has been interrupted by the simulated exception
        assertThat(awaitClose.join()).isEqualTo(State.LOOP_CLOSED_ON_ERROR);
        Throwable failure = loopFailureCause.get();
        assertThat(failure).isInstanceOf(KafkaException.class);
        assertThat(failure).hasMessageThat().isEqualTo("Unexpected exception during polling");
        assertThat(failure).hasCauseThat().isNotNull();
        assertThat(failure).hasCauseThat().isInstanceOf(IllegalStateException.class);
        assertThat(failure).hasCauseThat().hasMessageThat().isEqualTo("Fake Exception");

        // Verify that the internal resources have been closed
        assertThat(mockConsumer.closed()).isTrue();
        assertThat(wrapper.getRecordConsumer().isClosed()).isTrue();
        assertThat(wrapper.getMonitor().isRunning()).isFalse();

        // Try to shutdown again and verify that the status is still SHUTDOWN
        FutureStatus shutdown = wrapper.shutdown();
        assertThat(shutdown.isStateAvailable()).isTrue();
        assertThat(shutdown.join()).isEqualTo(State.SHUTDOWN);
    }

    private void updateOffsets(HashMap<TopicPartition, Long> offsets) {
        if (resetStrategy.equals(StrategyType.EARLIEST)) {
            mockConsumer.updateBeginningOffsets(offsets);
        } else {
            mockConsumer.updateEndOffsets(offsets);
        }
    }

    private void updateBeginAndEndOffsets(
            Map<TopicPartition, Long> beginOffsets, Map<TopicPartition, Long> endOffsets) {
        mockConsumer.updateBeginningOffsets(beginOffsets);
        mockConsumer.updateEndOffsets(endOffsets);
    }

    private static final Serializer<String> RECORD_SERIALIZER = new StringSerializer();

    private static ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset) {
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                RECORD_SERIALIZER.serialize(topic, "key-" + offset),
                RECORD_SERIALIZER.serialize(topic, "value-" + offset));
    }

    // --- Standalone mode tests ---

    private Config<String, String> makeStandaloneConfig() {
        try {
            return new Config<>(
                    "TestConnection",
                    makeProperties(),
                    Items.templatesFrom(makeTopicsConfig(false), String()),
                    ItemTemplatesUtils.fieldsExtractor(),
                    suppliers,
                    RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                    CommandModeStrategy.NONE,
                    new Concurrency(RecordConsumeWithOrderStrategy.UNORDERED, 2),
                    ConsumerGroupMode.STANDALONE);
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaConsumerWrapper<String, String> makeStandaloneWrapper(
            Set<String> availableTopics) {
        for (String topic : availableTopics) {
            this.mockConsumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }

        Config<String, String> config = makeStandaloneConfig();
        SubscribedItems subscribedItems = SubscribedItems.create();

        return new KafkaConsumerWrapper<String, String>(
                config, metadataListener, eventListener, subscribedItems, () -> this.mockConsumer);
    }

    @Test
    public void shouldAssignPartitionsInStandaloneMode() {
        String topic = "topic";
        KafkaConsumerWrapper<String, String> wrapper =
                makeStandaloneWrapper(Collections.singleton(topic));

        boolean result = wrapper.subscribed();
        assertThat(result).isTrue();

        // Verify partitions were assigned (not subscribed)
        assertThat(mockConsumer.assignment())
                .containsExactly(new TopicPartition(topic, 0));
    }

    @Test
    public void shouldFailAssignWhenNoPartitionsFound() {
        // Don't register any partitions for the topic
        Config<String, String> config = makeStandaloneConfig();
        SubscribedItems subscribedItems = SubscribedItems.create();

        KafkaConsumerWrapper<String, String> wrapper =
                new KafkaConsumerWrapper<>(
                        config,
                        metadataListener,
                        eventListener,
                        subscribedItems,
                        () -> this.mockConsumer);

        boolean result = wrapper.subscribed();
        assertThat(result).isFalse();
    }

    @Test
    public void shouldUseStandaloneOffsetService() {
        String topic = "topic";
        KafkaConsumerWrapper<String, String> wrapper =
                makeStandaloneWrapper(Collections.singleton(topic));

        OffsetService offsetService = wrapper.getOffsetService();
        assertThat(offsetService.getClass().getSimpleName())
                .isEqualTo("StandaloneOffsetServiceImpl");
    }
}
