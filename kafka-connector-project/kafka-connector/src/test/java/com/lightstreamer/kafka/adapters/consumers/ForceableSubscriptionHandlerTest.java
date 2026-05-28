
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
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.ForceableSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;

public class ForceableSubscriptionHandlerTest {

    private MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private ForceableSubscriptionsHandler<String, String> mkSubscriptionsHandler(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            CommandMode commandMode,
            String... topics) {

        Properties properties = new Properties();
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("bootstrap.servers", "localhost:9092");

        ConnectionSpec<String, String> spec =
                new ConnectionSpec<>(
                        "TestConnection",
                        properties,
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        new KafkaRecord.DeserializerPair<>(
                                OthersSelectorSuppliers.String()
                                        .keySelectorSupplier()
                                        .deserializer(),
                                OthersSelectorSuppliers.String()
                                        .valueSelectorSupplier()
                                        .deserializer()),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        commandMode,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        Function<Properties, Consumer<byte[], byte[]>> factory =
                props -> {
                    if (exceptionOnConnection) {
                        throw new KafkaException("Simulated Exception");
                    }

                    MockConsumer consumer = new MockConsumer(StrategyType.EARLIEST.toString());

                    if (exceptionOnListTopics) {
                        consumer.setListTopicException(
                                new KafkaException("Simulated Exception while listing topics"));
                    }

                    if (exceptionOnPoll) {
                        consumer.setPollException(
                                new KafkaException("Simulated Exception while polling"));
                    }

                    Map<TopicPartition, Long> partitionEndOffsets = new HashMap<>();
                    Map<TopicPartition, Long> partitionBeginOffsets = new HashMap<>();
                    for (String topic : topics) {
                        consumer.updatePartitions(
                                topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
                        partitionEndOffsets.put(new TopicPartition(topic, 0), 0L);
                        partitionBeginOffsets.put(new TopicPartition(topic, 0), 0L);
                        consumer.schedulePollTask(
                                () -> consumer.rebalance(Set.of(new TopicPartition(topic, 0))));
                    }
                    consumer.updateEndOffsets(partitionEndOffsets);
                    consumer.updateBeginningOffsets(partitionBeginOffsets);

                    return consumer;
                };

        SubscriptionsHandler.Builder<String, String> builder =
                SubscriptionsHandler.<String, String>builder()
                        .withConnectionSpec(spec)
                        .withConsumerFactory(factory)
                        .withMetadataListener(metadataListener)
                        .withItemSnapshotEnabled(true);
        return (ForceableSubscriptionsHandler<String, String>) builder.build();
    }

    private ForceableSubscriptionsHandler<String, String> subscriptionsHandler;
    private OnDemandSubscribedItems subscribedItems;
    private MockItemEventListener listener = new MockItemEventListener();

    void init(String... topics) {
        init(false, false, false, CommandMode.DISABLED, topics);
    }

    void init(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            String... topics) {
        init(
                exceptionOnConnection,
                exceptionOnListTopics,
                exceptionOnPoll,
                CommandMode.DISABLED,
                topics);
    }

    void init(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            CommandMode commandMode,
            String... topics) {
        this.subscriptionsHandler =
                mkSubscriptionsHandler(
                        exceptionOnConnection,
                        exceptionOnListTopics,
                        exceptionOnPoll,
                        commandMode,
                        topics);
        this.subscriptionsHandler.setListener(listener);
        //     this.subscribedItems = subscriptionsHandler.getSubscribedItems();
    }

    @Test
    public void shouldInit() {
        init("aTopic");
        assertThat(subscriptionsHandler.isConsuming()).isTrue();
    }

    @Test
    public void shouldFailInitDueToExceptionWhileConnecting() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(true, false, false, "aTopic"));
        assertThat(ke).hasMessageThat().isEqualTo("Simulated Exception");
        assertThat(subscriptionsHandler.joinCurrentState()).isEmpty();
    }

    @Test
    public void shouldFailInitDueToNonExistingTopics() {
        KafkaException ke = assertThrows(KafkaException.class, () -> init("nonExistingTopic"));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_BY_SUBSCRIPTION");
        assertThat(subscriptionsHandler.joinCurrentState())
                .hasValue(FutureStatus.State.INIT_FAILED_BY_SUBSCRIPTION);
    }

    @Test
    public void shouldFailInitDueToExceptionWhileGettingTopicList() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(false, true, false, "aTopic"));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_BY_EXCEPTION");
        assertThat(subscriptionsHandler.joinCurrentState())
                .hasValue(FutureStatus.State.INIT_FAILED_BY_EXCEPTION);
    }

    @Test
    public void shouldFailInitDueToExceptionWhilePolling() {
        KafkaException ke =
                assertThrows(KafkaException.class, () -> init(false, false, true, "aTopic"));
        assertThat(ke)
                .hasMessageThat()
                .isEqualTo("Consumer initialization failed: INIT_FAILED_BY_EXCEPTION");
        assertThat(subscriptionsHandler.joinCurrentState())
                .hasValue(FutureStatus.State.INIT_FAILED_BY_EXCEPTION);
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        init("aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionsHandler.subscribe("anItemTemplate", itemHandle1);
        assertThat(subscriptionsHandler.isConsuming()).isTrue();

        subscriptionsHandler.subscribe("anotherItemTemplate", itemHandle2);
        assertThat(subscriptionsHandler.isConsuming()).isTrue();

        // Verify that the items have been registered.
        OnDemandSubscribedItem item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isNotNull();
        assertThat(item1.canonicalName()).isEqualTo("anItemTemplate");

        OnDemandSubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(item2).isNotNull();
        assertThat(item2.canonicalName()).isEqualTo("anotherItemTemplate");

        // Verify that events are dispatched through the expected item handles.
        item1.clearSnapshot(listener);
        assertThat(listener.getSmartClearSnapshotCalls()).containsExactly(itemHandle1);

        listener.reset();

        item2.clearSnapshot(listener);
        assertThat(listener.getSmartClearSnapshotCalls()).containsExactly(itemHandle2);
    }

    // @Test
    // public void shouldFailSubscriptionDueToNonExistingTopics() throws SubscriptionException {
    //     init("nonExistingTopic");
    //     Object itemHandle = new Object();
    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle);

    //     // The subscribed topic does not exist on the broker, causing a delayed forced
    //     // unsubscription after the internal consumer has been created and the subscription
    //     // registered, so the future is completed with the corresponding failure status.
    //     assertThat(subscriptionsHandler.getFutureStatus().join())
    //             .isEqualTo(INIT_FAILED_BY_SUBSCRIPTION);
    //     assertThat(metadataListener.forcedUnsubscription()).isTrue();
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNotNull();

    //     // Yet the item is still registered.
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(1);

    //     // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
    //     // item.
    //     assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isPresent();
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(0);

    //     // After unsubscription, the handler should not be consuming anymore and the future
    // should
    //     // be reset to null.
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNull();
    //     assertThat(subscriptionsHandler.getFutureStatus()).isNull();
    // }

    // @Test
    // public void shouldFailSubscriptionDueToExceptionWhileGettingTopicList()
    //         throws SubscriptionException {
    //     init(false, true, false, "aTopic");
    //     Object itemHandle = new Object();
    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle);

    //     // The exception while getting the topic list causes a delayed forced unsubscription,
    //     // after the internal consumer has been created and the subscription registered, so the
    //     // future is completed with the corresponding failure status.
    //     assertThat(metadataListener.forcedUnsubscription()).isTrue();
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNotNull();
    //     assertThat(subscriptionsHandler.getFutureStatus().join())
    //             .isEqualTo(INIT_FAILED_BY_EXCEPTION);

    //     // Yet the item is still registered.
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(1);

    //     // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
    //     // item.
    //     assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isPresent();
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(0);

    //     // After unsubscription, the handler should not be consuming anymore and the future
    // should
    //     // be reset to null.
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNull();
    //     assertThat(subscriptionsHandler.getFutureStatus()).isNull();
    // }

    // @Test
    // public void shouldFailSubscriptionDueToExceptionWhileConnecting() throws
    // SubscriptionException {
    //     init(true, false, false);
    //     Object itemHandle = new Object();
    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle);

    //     // The exception while connecting to the broker causes an immediate forced
    // unsubscription,
    //     // without even creating the internal consumer, so the future is never completed and
    //     // remains null.
    //     assertThat(metadataListener.forcedUnsubscription()).isTrue();
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNull();
    //     assertThat(subscriptionsHandler.getFutureStatus()).isNull();

    //     // Yet the item is still registered.
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(1);

    //     // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
    //     // item.
    //     assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isPresent();
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(0);
    // }

    // @Test
    // public void shouldFailSubscriptionDueToExceptionWhilePolling()
    //         throws SubscriptionException, InterruptedException {
    //     init(false, false, true, "aTopic");
    //     Object itemHandle = new Object();
    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle);

    //     // The exception while polling causes a delayed forced unsubscription, after the
    //     // internal consumer has been created and the subscription registered, so the future
    //     // is completed with the corresponding failure status.
    //     assertThat(subscriptionsHandler.getFutureStatus().join())
    //             .isEqualTo(LOOP_CLOSED_BY_EXCEPTION);
    //     assertThat(metadataListener.forcedUnsubscription()).isTrue();
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNotNull();

    //     // Yet the item is still registered.
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(1);

    //     // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
    //     // item.
    //     assertThat(subscriptionsHandler.unsubscribe("anItemTemplate")).isPresent();
    //     assertThat(subscriptionsHandler.getSubscribedItems().size()).isEqualTo(0);

    //     // After unsubscription, the handler should not be consuming anymore and the future
    // should
    //     // be reset to null.
    //     assertThat(subscriptionsHandler.getConsumerWrapper()).isNull();
    //     assertThat(subscriptionsHandler.getFutureStatus()).isNull();
    // }

    // @Test
    // public void shouldSnapshotBeAvailable() {
    //     init(false, false, false, CommandMode.DISABLED, "aTopic");
    //     assertThat(subscriptionsHandler.isSnapshotAvailable("anyItem")).isEqualTo(true);
    // }

    // @Test
    // public void shouldUnsubscribe() throws SubscriptionException {
    //     init("aTopic");
    //     Object itemHandle1 = new Object();
    //     Object itemHandle2 = new Object();

    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle1);
    //     SubscribedItem item1 = subscribedItems.getItem("anItemTemplate");
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();

    //     subscriptionsHandler.subscribe("anotherItemTemplate", itemHandle2);
    //     SubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();

    //     Optional<SubscribedItem> removed1 = subscriptionsHandler.unsubscribe("anItemTemplate");
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();

    //     Optional<SubscribedItem> removed2 =
    // subscriptionsHandler.unsubscribe("anotherItemTemplate");
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(0);

    //     assertThat(removed1.get()).isSameInstanceAs(item1);
    //     assertThat(removed2.get()).isSameInstanceAs(item2);

    //     // After unsubscription, the handler should not be consuming anymore.
    //     assertThat(subscriptionsHandler.getFutureStatus()).isNull();
    // }

    // @Test
    // public void shouldNotUnsubscribeFromExistingItem() {
    //     init();

    //     Optional<SubscribedItem> unsubscribed =
    // subscriptionsHandler.unsubscribe("anItemTemplate");
    //     assertThat(unsubscribed).isEmpty();
    // }

    // @Test
    // public void shouldHandleSubscriptionBeforeShutdownCompletes()
    //         throws SubscriptionException, InterruptedException {
    //     init("aTopic");

    //     Object itemHandle = new Object();
    //     subscriptionsHandler.subscribe("anItemTemplate", itemHandle);
    //     TimeUnit.MILLISECONDS.sleep(50);
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();

    //     subscriptionsHandler.unsubscribe("anItemTemplate");
    //     CompletableFuture<Void> thread =
    //             CompletableFuture.runAsync(
    //                     () -> {
    //                         try {
    //                             subscriptionsHandler.subscribe("anotherItemTemplate",
    // itemHandle);
    //                         } catch (SubscriptionException e) {
    //                             throw new RuntimeException(e);
    //                         }
    //                     });
    //     thread.join();
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();
    // }

    // @Test
    // @Timeout(value = 5, unit = TimeUnit.SECONDS, threadMode = SEPARATE_THREAD)
    // public void shouldNotCloseConsumerOnConcurrentUnsubscribeAndSubscribe() throws Exception {
    //     init("aTopic");

    //     // Step 0: Subscribe item1 -> counter=1, consumer starts
    //     subscriptionsHandler.subscribe("anItemTemplate", new Object());
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue();

    //     // Latches to orchestrate the exact interleaving:
    //     //   Thread A: unsubscribe item1 -> counter=0 -> stopConsuming() starts
    //     //   Thread B: subscribe item2  -> counter=1 -> startConsuming() sees consumer!=null ->
    // nop
    //     //   Thread A: stopConsuming() finishes -> consumer=null
    //     //   Result: counter=1, consumer=null (dead consumer)
    //     CountDownLatch stopEntered = new CountDownLatch(1);
    //     CountDownLatch allowStopToFinish = new CountDownLatch(1);

    //     // Hook runs BEFORE stopConsuming() acquires the lock:
    //     // it signals that the unsubscribe path has committed to stopping,
    //     // then waits for Thread B to complete its subscribe + startConsuming(nop).
    //     subscriptionsHandler.stopConsumingHook =
    //             () -> {
    //                 stopEntered.countDown();
    //                 try {
    //                     allowStopToFinish.await();
    //                 } catch (InterruptedException e) {
    //                     Thread.currentThread().interrupt();
    //                 }
    //             };

    //     // Thread A: unsubscribe item1 -> counter becomes 0 -> enters stopConsuming() -> pauses
    //     CompletableFuture<Void> threadA =
    //             CompletableFuture.runAsync(
    //                     () -> {
    //                         subscriptionsHandler.unsubscribe("anItemTemplate");
    //                     });

    //     // Wait until Thread A has entered stopConsuming() (but hasn't acquired the lock yet).
    //     stopEntered.await();

    //     // Thread B: subscribe item2 -> counter becomes 1 -> startConsuming() acquires lock,
    //     // sees consumer != null -> "already consuming, nop" -> releases lock
    //     CompletableFuture<Void> threadB =
    //             CompletableFuture.runAsync(
    //                     () -> {
    //                         try {
    //                             subscriptionsHandler.subscribe("anotherItemTemplate", new
    // Object());
    //                         } catch (SubscriptionException e) {
    //                             throw new RuntimeException(e);
    //                         }
    //                     });
    //     threadB.join();

    //     // Now let Thread A finish: it acquires the lock, shuts down consumer, sets
    // consumer=null.
    //     allowStopToFinish.countDown();
    //     threadA.join();

    //     // At this point: counter=1, but consumer has been shut down.
    //     // The handler SHOULD still be consuming (counter > 0), but the bug leaves it dead.
    //     assertThat(subscriptionsHandler.getItemsCounter()).isEqualTo(1);
    //     assertThat(subscriptionsHandler.isConsuming()).isTrue(); // FAILS before fix
    // }
}
