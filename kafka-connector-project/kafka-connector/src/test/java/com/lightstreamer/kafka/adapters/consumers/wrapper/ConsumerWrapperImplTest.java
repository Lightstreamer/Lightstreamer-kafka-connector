
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

import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper.AdminInterface;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockTriggerConfig;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ConsumerWrapperImplTest {

    private MockMetadataListener metadataListener;
    private MockItemEventListener itemEventListener;
    private MockTriggerConfig config;
    private Collection<SubscribedItem> subscribedItems = Collections.emptyList();
    private MockConsumer<String, String> mockConsumer;
    private final OffsetResetStrategy resetStrategy = OffsetResetStrategy.EARLIEST;

    @BeforeEach
    public void setUp() {
        metadataListener = new Mocks.MockMetadataListener();
        itemEventListener = new Mocks.MockItemEventListener();

        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(),
                        List.of(
                                TopicMappingConfig.fromDelimitedMappings("topic", "item"),
                                TopicMappingConfig.fromDelimitedMappings("topic2", "item")));

        mockConsumer = new MockConsumer<>(resetStrategy);

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                resetStrategy.equals(EARLIEST) ? "earliest" : "latest");
        config = new Mocks.MockTriggerConfig(topicsConfig, properties);
    }

    ConsumerWrapperImpl<String, String> mkConsumerWrapper(AdminInterface admin) {
        return (ConsumerWrapperImpl<String, String>)
                ConsumerWrapper.create(
                        config,
                        itemEventListener,
                        metadataListener,
                        subscribedItems,
                        () -> mockConsumer,
                        p -> admin);
    }

    @Test
    public void shouldNotSubscribeDueToNotExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface("anotherTopic"));
        assertThat(consumer.subscribed()).isFalse();
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldNotSubscribeDueToExceptionWhileCheckintExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface("topic", true));
        assertThat(consumer.subscribed()).isFalse();
        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldSubscribe() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // A rebalance must be scheduled to later use the subscribe method
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Set.of(partition0, partition1)));

        // Set the start offset for each partition
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(partition0, 0L);
        offsets.put(partition1, 0L);
        mockConsumer.updateBeginningOffsets(offsets);

        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));
        assertThat(consumer.subscribed()).isTrue();
        assertThat(mockConsumer.subscription()).containsExactly(topic);
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldInitAndStore() {
        String topic = "topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

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
                Records.generateRecords(topic, 10, List.of("a", "b"), new int[] {0, 1});

        // Invoke the method and verify that task has been actuallyinvoked with the expected
        // simulated records
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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), new int[] {0, 1});
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
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.current();
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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), new int[] {0, 1});
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
        assertThat(ce.getCause()).isInstanceOf(KafkaException.class);

        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        OffsetStore offsetStore = consumer.getOffsetService().offsetStore().get();
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.current();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @Test
    public void shouldClose() throws InterruptedException {
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
                mkConsumerWrapper(new Mocks.MockAdminInterface(topic));

        // Generate then simulated records to be polled from the mocked consumer
        ConsumerRecords<String, String> records =
                Records.generateRecords(topic, 100, List.of("a", "b"), new int[] {0, 1});
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
        Map<TopicPartition, OffsetAndMetadata> map = offsetStore.current();
        assertThat(map.keySet()).containsExactly(partition0, partition1);
        // Verify that offsets have been moved reasonably
        assertThat(map.get(partition0).offset()).isGreaterThan(records.count() / 3);
        assertThat(map.get(partition1).offset()).isGreaterThan(records.count() / 3);
    }

    @Test
    public void shouldRunWithNoSubscriptioneDueToNotExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface("anotherTopic"));

        consumer.run();

        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldRunWithNoSubscriptioneDueToExceptionWhileCheckintExistingTopic() {
        ConsumerWrapperImpl<String, String> consumer =
                mkConsumerWrapper(new Mocks.MockAdminInterface("anotherTopic", true));

        consumer.run();

        assertThat(mockConsumer.subscription()).isEmpty();
        assertThat(mockConsumer.closed());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }
}
