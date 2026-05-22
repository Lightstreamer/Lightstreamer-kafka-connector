
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
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State.INIT_FAILED_BY_EXCEPTION;
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State.INIT_FAILED_BY_SUBSCRIPTION;
import static com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State.LOOP_CLOSED_BY_EXCEPTION;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.OnDemandSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

public class OnDemandSubscriptionsHandlerTest {

    private MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private OnDemandSubscriptionsHandler<String, String> mkSubscriptionsHandler(
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
                        consumer.setListTopicException(new KafkaException("Simulated Exception"));
                    }

                    if (exceptionOnPoll) {
                        consumer.setPollException(new KafkaException("Simulated Exception"));
                    }

                    for (String topic : topics) {
                        consumer.updatePartitions(
                                topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
                    }
                    return consumer;
                };

        SubscriptionsHandler.Builder<String, String> builder =
                SubscriptionsHandler.<String, String>builder()
                        .withConnectionSpec(spec)
                        .withConsumerFactory(factory)
                        .withMetadataListener(metadataListener);
        return (OnDemandSubscriptionsHandler<String, String>) builder.build();
    }

    private OnDemandSubscriptionsHandler<String, String> subscriptionHandler;
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
        this.subscriptionHandler =
                mkSubscriptionsHandler(
                        exceptionOnConnection,
                        exceptionOnListTopics,
                        exceptionOnPoll,
                        commandMode,
                        topics);
        this.subscriptionHandler.setListener(listener);
        this.subscribedItems = subscriptionHandler.getSubscribedItems();
    }

    @Test
    public void shouldInit() {
        init();
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException, InterruptedException {
        init("aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(2);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        // Verify that the items have been registered.
        OnDemandSubscribedItem item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isNotNull();
        assertThat(item1.canonicalName()).isEqualTo("anItemTemplate");
        assertThat(item1.itemHandle()).isSameInstanceAs(itemHandle1);

        OnDemandSubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(item2).isNotNull();
        assertThat(item2.canonicalName()).isEqualTo("anotherItemTemplate");
        assertThat(item2.itemHandle()).isSameInstanceAs(itemHandle2);
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init();
        Object itemHandle = new Object();

        // The item name does not match any configured template, triggering a SubscriptionException
        // that causes an immediate failure without creating the internal consumer, so the future
        // is never completed and remains null.
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();

        // Since no subscription was actually registered, the handler should not be consuming and
        // no forced unsubscription should have been triggered.
        assertThat(metadataListener.forcedUnsubscription()).isFalse();

        // Any attempt to unsubscribe should return an empty result, as no subscription was
        // registered.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isEmpty();
    }

    @Test
    public void shouldFailSubscriptionDueToInvalidExpression() {
        init();
        Object itemHandle = new Object();

        // The item name contains invalid expression syntax, triggering an ExpressionException
        // that is wrapped into a SubscriptionException, causing an immediate failure without
        // creating the internal consumer, so the future is never completed and remains null.
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();

        // Since no subscription was actually registered, the handler should not be consuming and
        // no forced unsubscription should have been triggered.
        assertThat(metadataListener.forcedUnsubscription()).isFalse();

        // Any attempt to unsubscribe should return an empty result, as no subscription was
        // registered.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isEmpty();
    }

    @Test
    public void shouldFailSubscriptionDueToNonExistingTopics() throws SubscriptionException {
        init("nonExistingTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // The subscribed topic does not exist on the broker, causing a delayed forced
        // unsubscription after the internal consumer has been created and the subscription
        // registered, so the future is completed with the corresponding failure status.
        assertThat(subscriptionHandler.getFutureStatus().join())
                .isEqualTo(INIT_FAILED_BY_SUBSCRIPTION);
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(subscriptionHandler.getConsumerWrapper()).isNotNull();

        // Yet the item is still registered.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
        // item.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isPresent();
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(0);

        // After unsubscription, the handler should not be consuming anymore and the future should
        // be reset to null.
        assertThat(subscriptionHandler.getConsumerWrapper()).isNull();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();
    }

    @Test
    public void shouldFailSubscriptionDueToExceptionWhileGettingTopicList()
            throws SubscriptionException {
        init(false, true, false, "aTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // The exception while getting the topic list causes a delayed forced unsubscription,
        // after the internal consumer has been created and the subscription registered, so the
        // future is completed with the corresponding failure status.
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(subscriptionHandler.getConsumerWrapper()).isNotNull();
        assertThat(subscriptionHandler.getFutureStatus().join())
                .isEqualTo(INIT_FAILED_BY_EXCEPTION);

        // Yet the item is still registered.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
        // item.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isPresent();
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(0);

        // After unsubscription, the handler should not be consuming anymore and the future should
        // be reset to null.
        assertThat(subscriptionHandler.getConsumerWrapper()).isNull();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();
    }

    @Test
    public void shouldFailSubscriptionDueToExceptionWhileConnecting() throws SubscriptionException {
        init(true, false, false);
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // The exception while connecting to the broker causes an immediate forced unsubscription,
        // without even creating the internal consumer, so the future is never completed and
        // remains null.
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(subscriptionHandler.getConsumerWrapper()).isNull();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();

        // Yet the item is still registered.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
        // item.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isPresent();
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(0);
    }

    @Test
    public void shouldFailSubscriptionDueToExceptionWhilePolling()
            throws SubscriptionException, InterruptedException {
        init(false, false, true, "aTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // The exception while polling causes a delayed forced unsubscription, after the
        // internal consumer has been created and the subscription registered, so the future
        // is completed with the corresponding failure status.
        assertThat(subscriptionHandler.getFutureStatus().join())
                .isEqualTo(LOOP_CLOSED_BY_EXCEPTION);
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        assertThat(subscriptionHandler.getConsumerWrapper()).isNotNull();

        // Yet the item is still registered.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        // Following the forced unsubscription, the Kernel will call unsubscribe to clean up the
        // item.
        assertThat(subscriptionHandler.unsubscribe("anItemTemplate")).isPresent();
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(0);

        // After unsubscription, the handler should not be consuming anymore and the future should
        // be reset to null.
        assertThat(subscriptionHandler.getConsumerWrapper()).isNull();
        assertThat(subscriptionHandler.getFutureStatus()).isNull();
    }

    static Stream<Arguments> commandModes() {
        return Stream.of(
                Arguments.of(CommandMode.DISABLED, false),
                Arguments.of(CommandMode.EXPLICIT, true),
                Arguments.of(CommandMode.AUTO, false));
    }

    @ParameterizedTest
    @MethodSource("commandModes")
    public void shouldGetSnapshotAvailability(CommandMode commandMode, boolean expected)
            throws SubscriptionException {
        init(false, false, false, commandMode, "aTopic");
        assertThat(subscriptionHandler.isSnapshotAvailable("anItem")).isEqualTo(expected);
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        init("aTopic");
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        SubscribedItem item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        SubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        Optional<SubscribedItem> removed1 = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        Optional<SubscribedItem> removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);

        assertThat(removed1.get()).isSameInstanceAs(item1);
        assertThat(removed2.get()).isSameInstanceAs(item2);

        // After unsubscription, the handler should not be consuming anymore.
        assertThat(subscriptionHandler.getFutureStatus()).isNull();
    }

    @Test
    public void shouldNotUnsubscribeFromExistingItem() {
        init();

        Optional<SubscribedItem> unsubscribed = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(unsubscribed).isEmpty();
    }

    @Test
    public void shouldHandleSubscriptionBeforeShutdownCompletes()
            throws SubscriptionException, InterruptedException {
        init("aTopic");

        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);
        TimeUnit.MILLISECONDS.sleep(50);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        subscriptionHandler.unsubscribe("anItemTemplate");
        CompletableFuture<Void> thread =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                subscriptionHandler.subscribe("anotherItemTemplate", itemHandle);
                            } catch (SubscriptionException e) {
                                throw new RuntimeException(e);
                            }
                        });
        thread.join();
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS, threadMode = SEPARATE_THREAD)
    public void shouldNotCloseConsumerOnConcurrentUnsubscribeAndSubscribe() throws Exception {
        init("aTopic");

        // Step 0: Subscribe item1 -> counter=1, consumer starts
        subscriptionHandler.subscribe("anItemTemplate", new Object());
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        // Latches to orchestrate the exact interleaving:
        //   Thread A: unsubscribe item1 -> counter=0 -> stopConsuming() starts
        //   Thread B: subscribe item2  -> counter=1 -> startConsuming() sees consumer!=null -> nop
        //   Thread A: stopConsuming() finishes -> consumer=null
        //   Result: counter=1, consumer=null (dead consumer)
        CountDownLatch stopEntered = new CountDownLatch(1);
        CountDownLatch allowStopToFinish = new CountDownLatch(1);

        // Hook runs BEFORE stopConsuming() acquires the lock:
        // it signals that the unsubscribe path has committed to stopping,
        // then waits for Thread B to complete its subscribe + startConsuming(nop).
        subscriptionHandler.stopConsumingHook =
                () -> {
                    stopEntered.countDown();
                    try {
                        allowStopToFinish.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

        // Thread A: unsubscribe item1 -> counter becomes 0 -> enters stopConsuming() -> pauses
        CompletableFuture<Void> threadA =
                CompletableFuture.runAsync(
                        () -> {
                            subscriptionHandler.unsubscribe("anItemTemplate");
                        });

        // Wait until Thread A has entered stopConsuming() (but hasn't acquired the lock yet).
        stopEntered.await();

        // Thread B: subscribe item2 -> counter becomes 1 -> startConsuming() acquires lock,
        // sees consumer != null -> "already consuming, nop" -> releases lock
        CompletableFuture<Void> threadB =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                subscriptionHandler.subscribe("anotherItemTemplate", new Object());
                            } catch (SubscriptionException e) {
                                throw new RuntimeException(e);
                            }
                        });
        threadB.join();

        // Now let Thread A finish: it acquires the lock, shuts down consumer, sets consumer=null.
        allowStopToFinish.countDown();
        threadA.join();

        // At this point: counter=1, but consumer has been shut down.
        // The handler SHOULD still be consuming (counter > 0), but the bug leaves it dead.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue(); // FAILS before fix
    }
}
