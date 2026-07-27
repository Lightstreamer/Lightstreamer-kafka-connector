
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
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State.LOOP_CLOSED_ON_ERROR;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ConsumerMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.RecordPipeline;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.RecordPipeline.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.ForceableSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.ForceableSubscribedItems;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

class ForceableSubscriptionHandlerTest {

    // Configured broker topic.
    private static final String TOPIC = "aTopic";

    private ForceableSubscriptionsHandler<String, String> subscriptionsHandler;
    private MockItemEventListener listener = new MockItemEventListener();
    private MockConsumer consumer;

    private ForceableSubscriptionsHandler<String, String> mkSubscriptionsHandler(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            int maxIdleSeconds,
            String templateTopic) {

        Properties properties = new Properties();
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("bootstrap.servers", "localhost:9092");

        ConnectionSpec<String, String> spec =
                new ConnectionSpec<>(
                        "TestConnection",
                        properties,
                        new KafkaRecord.DeserializerPair<>(
                                OthersSelectorSuppliers.String()
                                        .keySelectorSupplier()
                                        .deserializer(),
                                OthersSelectorSuppliers.String()
                                        .valueSelectorSupplier()
                                        .deserializer()),
                        ConsumerMode.GROUP,
                        RecordConsumeFrom.EARLIEST,
                        new RecordPipeline<>(
                                ItemTemplatesUtils.itemTemplates(
                                        templateTopic, "anItemTemplate,anotherItemTemplate"),
                                ItemTemplatesUtils.fieldsExtractor(),
                                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                                false,
                                new Concurrency(
                                        RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1)));

        Function<Properties, Consumer<byte[], byte[]>> factory =
                props -> {
                    if (exceptionOnConnection) {
                        throw new KafkaException("Simulated Exception");
                    }

                    consumer = new MockConsumer(StrategyType.EARLIEST.toString());

                    if (exceptionOnListTopics) {
                        consumer.setListTopicException(
                                new KafkaException("Simulated Exception while listing topics"));
                    }

                    if (exceptionOnPoll) {
                        consumer.setPollException(
                                new KafkaException("Simulated Exception while polling"));
                    }

                    consumer.updatePartitions(
                            TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
                    Map<TopicPartition, Long> partitionEndOffsets = new HashMap<>();
                    Map<TopicPartition, Long> partitionBeginOffsets = new HashMap<>();
                    partitionEndOffsets.put(new TopicPartition(TOPIC, 0), 0L);
                    partitionBeginOffsets.put(new TopicPartition(TOPIC, 0), 0L);
                    consumer.schedulePollTask(
                            () -> consumer.rebalance(Set.of(new TopicPartition(TOPIC, 0))));
                    consumer.updateEndOffsets(partitionEndOffsets);
                    consumer.updateBeginningOffsets(partitionBeginOffsets);

                    return consumer;
                };

        SubscriptionsHandler.Builder<String, String> builder =
                SubscriptionsHandler.<String, String>builder()
                        .connectionSpec(spec)
                        .consumerFactory(factory)
                        .snapshotEnabled(true)
                        .itemSnapshotMaxIdleSeconds(maxIdleSeconds);
        return (ForceableSubscriptionsHandler<String, String>) builder.build();
    }

    void init(String templateTopic) {
        init(false, false, false, 0, templateTopic);
    }

    void init(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            String templateTopic) {
        init(exceptionOnConnection, exceptionOnListTopics, exceptionOnPoll, 0, templateTopic);
    }

    void init(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            int maxIdleSeconds,
            String templateTopic) {
        this.subscriptionsHandler =
                mkSubscriptionsHandler(
                        exceptionOnConnection,
                        exceptionOnListTopics,
                        exceptionOnPoll,
                        maxIdleSeconds,
                        templateTopic);
        this.subscriptionsHandler.setListener(listener);
    }

    @Test
    void shouldInit() {
        init(TOPIC);
        // Unavailable state is expected while the consumer is performing the infinite polling loop
        // in the background
        assertThat(subscriptionsHandler.getLifecycleStatus().isStateAvailable()).isFalse();
        assertThat(subscriptionsHandler.getItemSnapshotMaxIdleSeconds()).isEqualTo(0);
        assertThat(subscriptionsHandler.getScheduled()).isEmpty();
    }

    @Test
    void shouldStartIdleSnapshotScheduler() {
        init(false, false, false, 30, TOPIC);
        assertThat(subscriptionsHandler.getLifecycleStatus().isStateAvailable()).isFalse();
        assertThat(subscriptionsHandler.getItemSnapshotMaxIdleSeconds()).isEqualTo(30);
        assertThat(subscriptionsHandler.getScheduled()).isPresent();
    }

    @Test
    void shouldGetSnapshotAvailability() {
        init(TOPIC);
        assertThat(subscriptionsHandler.isSnapshotAvailable("anyItem")).isTrue();
    }

    @Test
    void shouldFailInitDueToExceptionWhileConnecting() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(true, false, false, TOPIC));
        assertThat(ke).hasMessageThat().isEqualTo("Simulated Exception");
    }

    @Test
    void shouldFailInitDueToNonExistingTopics() {
        KafkaException ke = assertThrows(KafkaException.class, () -> init("nonExistingTopic"));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_ON_MISSING_TOPICS");
    }

    @Test
    void shouldFailInitDueToExceptionWhileGettingTopicList() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(false, true, false, TOPIC));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_ON_ERROR");
    }

    @Test
    void shouldFailInitDueToExceptionWhilePollingInTheCatchupPhase() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(false, false, true, TOPIC));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_ON_ERROR");
        assertThat(listener.getFailures()).isEmpty();
    }

    @Test
    void shouldFailInitDueToExceptionWhilePolling() {
        init(false, false, false, 0, TOPIC);

        // Simulate exception while polling after initialization (including catch-up) completes
        // successfully.
        consumer.setPollException(new KafkaException("Simulated Exception while polling"));

        assertThat(subscriptionsHandler.getLifecycleStatus().join())
                .isEqualTo(LOOP_CLOSED_ON_ERROR);
        assertThat(listener.getFailures()).hasSize(1);
        assertThat(listener.getFailures().get(0))
                .hasMessageThat()
                .isEqualTo("Simulated Exception while polling");
    }

    @Test
    void shouldSubscribeAndUnsubscribe() throws SubscriptionException {
        init(TOPIC);

        Object itemHandle = new Object();

        // Path-1 organic subscribe installs a new entry and immediately closes snapshot delivery.
        subscriptionsHandler.subscribe("anItemTemplate", itemHandle);
        assertThat(listener.getSmartEndOfSnapshotCalls()).containsExactly(itemHandle);

        assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isTrue();
    }

    @Test
    void shouldNotUnsubscribeAfterItemIsPromotedToForced() throws SubscriptionException {
        init(TOPIC);

        Object itemHandle = new Object();
        subscriptionsHandler.subscribe("anItemTemplate", itemHandle);

        // Simulate record-driven lookup: hit-on-unforced promotes Path-1 entry to forced.
        ForceableSubscribedItems subscribedItems = subscriptionsHandler.getSubscribedItems();
        assertThat(subscribedItems.getItem("anItemTemplate")).isNotNull();

        assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isFalse();
        assertThat(subscribedItems.getItem("anItemTemplate")).isNotNull();
    }

    @Test
    void shouldNotUnsubscribeFromExistingItem() {
        init(TOPIC);
        assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isFalse();
    }
}
