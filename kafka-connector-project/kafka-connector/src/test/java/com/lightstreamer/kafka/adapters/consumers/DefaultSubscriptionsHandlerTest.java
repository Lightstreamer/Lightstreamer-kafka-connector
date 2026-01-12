
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
import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DefaultSubscriptionsHandlerTest {

    private MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private DefaultSubscriptionsHandler<String, String> mkSubscriptionsHandler(
            boolean exceptionOnConnection, String... topics) {

        Properties properties = new Properties();
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
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

        Mocks.MockConsumer<Deferred<String>, Deferred<String>> consumer =
                new Mocks.MockConsumer<>(OffsetResetStrategy.EARLIEST);
        for (String topic : topics) {
            consumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }

        Supplier<Consumer<Deferred<String>, Deferred<String>>> supplier =
                () -> {
                    if (exceptionOnConnection) {
                        throw new KafkaException("Simulated Exception");
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
        init(false, topics);
    }

    void init(boolean exceptionOnConnection, String... topics) {
        this.subscriptionHandler = mkSubscriptionsHandler(exceptionOnConnection, topics);
        this.subscriptionHandler.setListener(listener);
        this.subscribedItems = subscriptionHandler.getSubscribedItems();
    }

    @Test
    public void shouldInit() {
        init();
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.consumeAtStartup()).isFalse();
        assertThat(subscriptionHandler.allowImplicitItems()).isFalse();
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(subscriptionHandler.getSubscribedItems().isEmpty()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
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
        Optional<SubscribedItem> item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isPresent();
        assertThat(item1.get()).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle1));

        Optional<SubscribedItem> item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(item2).isPresent();
        assertThat(item2.get()).isEqualTo(Items.subscribedFrom("anotherItemTemplate", itemHandle2));
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
    @Timeout(value = 1, unit = TimeUnit.SECONDS, threadMode = SEPARATE_THREAD)
    public void shouldForceUnsubscriptionDueToExceptionWhileConnecting()
            throws SubscriptionException {
        init(true);
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
        init();
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        Optional<SubscribedItem> item1 = subscribedItems.getItem("anItemTemplate");

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        Optional<SubscribedItem> item2 = subscribedItems.getItem("anotherItemTemplate");

        Optional<SubscribedItem> removed1 = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        Optional<SubscribedItem> removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);

        assertThat(removed1.get()).isSameInstanceAs(item1.get());
        assertThat(removed2.get()).isSameInstanceAs(item2.get());

        // After unsubscription, the handler should not be consuming anymore
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldNotUnsubscribeNonExistingItem() {
        init();

        Optional<SubscribedItem> unsubscribed = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(unsubscribed).isEmpty();
    }
}
