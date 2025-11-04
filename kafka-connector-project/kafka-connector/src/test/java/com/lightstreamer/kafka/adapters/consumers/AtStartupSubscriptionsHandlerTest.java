
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

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.AtStartupSubscriptionsHandler;
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

public class AtStartupSubscriptionsHandlerTest {

    private static AtStartupSubscriptionsHandler<String, String> mkSubscriptionsHandler(
            boolean exceptionOnConnection, boolean allowImplicitItems, String... topics) {

        Properties properties = new Properties();
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        Config<String, String> config =
                new Config<>(
                        "TestConnection",
                        properties,
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        OthersSelectorSuppliers.String().deserializers(),
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

        return new AtStartupSubscriptionsHandler<>(
                config, new Mocks.MockMetadataListener(), allowImplicitItems, supplier);
    }

    private AtStartupSubscriptionsHandler<String, String> subscriptionHandler;
    private SubscribedItems subscribedItems;
    private MockItemEventListener listener;

    void init(boolean allowImplicitItems, String... topics) {
        init(false, allowImplicitItems, topics);
    }

    void init(boolean exceptionOnConnection, boolean allowImplicitItems, String... topics) {
        this.subscriptionHandler =
                mkSubscriptionsHandler(exceptionOnConnection, allowImplicitItems, topics);
        this.listener = new Mocks.MockItemEventListener();
        this.subscriptionHandler.setListener(listener);
        this.subscribedItems = subscriptionHandler.getSubscribedItems();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldInit(boolean allowImplicitItems) throws InterruptedException {
        init(allowImplicitItems, "aTopic");

        assertThat(listener.getFailure()).isNull();
        assertThat(subscriptionHandler.consumeAtStartup()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isTrue();
        assertThat(subscriptionHandler.allowImplicitItems()).isEqualTo(allowImplicitItems);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFailOnInitDueToNonExistingTopic(boolean allowImplicitItems) {
        init(allowImplicitItems, "notExistingTopic");

        Throwable failure = listener.getFailure();
        assertThat(failure).isNotNull();
        assertThat(failure).hasMessageThat().isEqualTo("Failed to start consuming from Kafka");
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFailOnInitDueToExceptionWhileConnecting(boolean allowImplicitItems) {
        init(true, false, "aTopic");

        Throwable failure = listener.getFailure();
        assertThat(failure).isNotNull();
        assertThat(failure).isInstanceOf(KafkaException.class);
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldSubscribeWhenNotAllowImplicitItems() throws SubscriptionException {
        init(false, "aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);

        // Verify that the items have been registered
        Optional<SubscribedItem> item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isPresent();
        assertThat(item1.get()).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle1));

        Optional<SubscribedItem> item2 = subscribedItems.getItem("anotherItemTemplate");
        assertThat(item2).isPresent();
        assertThat(item2.get()).isEqualTo(Items.subscribedFrom("anotherItemTemplate", itemHandle2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFailSubscriptionDueToNotRegisteredTemplate(boolean allowImplicitItems) {
        init(allowImplicitItems, "aTopic");

        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");
    }

    @Test
    public void shouldNotSubscribeWhenAllowImplicitItems() throws SubscriptionException {
        init(true, "aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);

        // Verify that the item has not been registered
        Optional<SubscribedItem> item1 = subscribedItems.getItem("anItemTemplate");
        assertThat(item1).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFailSubscriptionDueInvalidExpression(boolean allowImplicitItems) {
        init(allowImplicitItems, "aTopic");

        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");
    }

    @Test
    public void shouldUnsubscribeWhenNotAllowImplicitItems() throws SubscriptionException {
        init(false, "aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        Optional<SubscribedItem> item1 = subscribedItems.getItem("anItemTemplate");

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        Optional<SubscribedItem> item2 = subscribedItems.getItem("anotherItemTemplate");

        Optional<SubscribedItem> removed1 = subscriptionHandler.unsubscribe("anItemTemplate");
        Optional<SubscribedItem> removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");

        assertThat(removed1.get()).isSameInstanceAs(item1.get());
        assertThat(removed2.get()).isSameInstanceAs(item2.get());

        // Even after unsubscription, the handler should still be consuming
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotUnsubscribe(boolean allowImplicitItems) {
        init(allowImplicitItems, "aTopic");

        Optional<SubscribedItem> unsubscribed = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(unsubscribed).isEmpty();
    }
}
