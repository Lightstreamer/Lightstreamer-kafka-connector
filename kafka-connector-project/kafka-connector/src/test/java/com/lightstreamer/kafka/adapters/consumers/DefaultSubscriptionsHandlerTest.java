
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
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.DefaultSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
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

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DefaultSubscriptionsHandlerTest {

    private MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private DefaultSubscriptionsHandler<String, String> mkSubscriptionsHandler(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            String... topics) {

        Properties properties = new Properties();
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("bootstrap.servers", "localhost:9092");

        Config<String, String> config =
                new Config<>(
                        "TestConnection",
                        properties,
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        OthersSelectorSuppliers.String(),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        CommandModeStrategy.NONE,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        Supplier<Consumer<byte[], byte[]>> supplier =
                () -> {
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
                        .withConsumerConfig(config)
                        .withConsumerSupplier(supplier)
                        .withMetadataListener(metadataListener);
        return new DefaultSubscriptionsHandler<>(builder);
    }

    private DefaultSubscriptionsHandler<String, String> subscriptionHandler;
    private SubscribedItems subscribedItems;
    private MockItemEventListener listener = new MockItemEventListener();

    void init(String... topics) {
        init(false, false, false, topics);
    }

    void init(
            boolean exceptionOnConnection,
            boolean exceptionOnListTopics,
            boolean exceptionOnPoll,
            String... topics) {
        this.subscriptionHandler =
                mkSubscriptionsHandler(
                        exceptionOnConnection, exceptionOnListTopics, exceptionOnPoll, topics);
        this.subscriptionHandler.setListener(listener);
        this.subscribedItems = subscriptionHandler.getSubscribedItems();
    }

    @Test
    public void shouldInit() {
        init();
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
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

        // Verify that the items have been registered
        SubscribedItem item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isNotNull();
        assertThat(item1).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle1));

        SubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(item2).isNotNull();
        assertThat(item2).isEqualTo(Items.subscribedFrom("anotherItemTemplate", itemHandle2));
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init();
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS, threadMode = SEPARATE_THREAD)
    public void shouldForceUnsubscriptionWhenSubscribeToNonExistingTopics()
            throws SubscriptionException {
        init("nonExistingTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);
        assertThat(subscriptionHandler.getFutureStatus().join().initFailed());

        // Removal of subscribed item actual happens in the unsubscribe method
        // invoked by the Kernel following the forced unsubscription
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldForceUnsubscriptionDueToExceptionWhileGettingTopicList()
            throws SubscriptionException {
        init(false, true, false, "aTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // Removal of subscribed item actual happens in the unsubscribe method
        // invoked by the Kernel following the forced unsubscription
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldForceUnsubscriptionDueToExceptionWhileConnecting()
            throws SubscriptionException {
        init(true, false, false);
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // Removal of subscribed item actual happens in the unsubscribe method
        // invoked by the Kernel following the forced unsubscription
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldForceUnsubscriptionDueToExceptionWhileFirstPoll()
            throws SubscriptionException {
        init(false, false, true, "aTopic");
        Object itemHandle = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle);

        // Removal of subscribed item actual happens in the unsubscribe method
        // invoked by the Kernel following the forced unsubscription
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.getSubscribedItems().size()).isEqualTo(1);

        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldFailSubscriptionDueInvalidExpression() {
        init();
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
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

        // After unsubscription, the handler should not be consuming anymore
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldNotUnsubscribeNonExistingItem() {
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
    public void shouldNotLoseConsumerOnConcurrentUnsubscribeAndSubscribe() throws Exception {
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

        // Wait until Thread A has entered stopConsuming() (but hasn't acquired the lock yet)
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

        // Now let Thread A finish: it acquires the lock, shuts down consumer, sets consumer=null
        allowStopToFinish.countDown();
        threadA.join();

        // At this point: counter=1, but consumer has been shut down.
        // The handler SHOULD still be consuming (counter > 0), but the bug leaves it dead.
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue(); // FAILS before fix
    }
}
