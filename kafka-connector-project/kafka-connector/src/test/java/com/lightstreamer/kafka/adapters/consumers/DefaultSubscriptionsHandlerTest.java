
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandlerSupport.DefaultSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.Config;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockMetadataListener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Properties;
import java.util.stream.Stream;

public class DefaultSubscriptionsHandlerTest {

    private static DefaultSubscriptionsHandler mkSubscriptionsHandler(
            MetadataListener metadataListener,
            boolean throwExceptionWhileConnectingToKafka,
            CommandModeStrategy commandModeStrategy) {

        Config<String, String> config =
                new Config<>(
                        "TestConnection",
                        new Properties(),
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        null,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        commandModeStrategy,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        KafkaConsumerWrapper consumerWrapper =
                KafkaConsumerWrapper.create(
                        config,
                        metadataListener,
                        new Mocks.MockItemEventListener(),
                        Mocks.MockConsumer.supplier(throwExceptionWhileConnectingToKafka));
        return new DefaultSubscriptionsHandler(consumerWrapper);
    }

    private DefaultSubscriptionsHandler subscriptionHandler;
    private MockMetadataListener metadataListener;

    void init() {
        init(false, CommandModeStrategy.NONE);
    }

    void init(
            boolean throwExceptionWhileConnectingToKafka, CommandModeStrategy commandModeStrategy) {
        metadataListener = new Mocks.MockMetadataListener();
        subscriptionHandler =
                mkSubscriptionsHandler(
                        metadataListener,
                        throwExceptionWhileConnectingToKafka,
                        commandModeStrategy);
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        init();
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        assertThat(subscriptionHandler.isConsuming()).isFalse();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(2);
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        Item item1 = subscriptionHandler.getSubscribedItem("anItemTemplate");
        assertThat(item1).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle1));

        Item item2 = subscriptionHandler.getSubscribedItem("anotherItemTemplate");
        assertThat(item2).isEqualTo(Items.subscribedFrom("anotherItemTemplate", itemHandle2));

        // assertThat(kafkaConsumer.hasRan()).isTrue();
        // assertThat(kafkaConsumer.isClosed()).isFalse();
    }

    static Stream<Arguments> snapshotHandlers() {
        return Stream.of(
                arguments(CommandModeStrategy.NONE, false),
                arguments(CommandModeStrategy.AUTO, false),
                arguments(CommandModeStrategy.ENFORCE, true));
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init();
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se.getMessage()).isEqualTo("Invalid Item");
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldFailSubscriptionDueInvalidExpression() {
        init();
        Object itemHandle = new Object();
        assertThrows(
                SubscriptionException.class,
                () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldFailSubscriptionDueToKafkaException() throws SubscriptionException {
        init(true, CommandModeStrategy.NONE);
        Object itemHandle = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle);
        Item item = subscriptionHandler.getSubscribedItem("anItemTemplate");

        assertThat(item).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle));
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        // assertThat(kafkaConsumer.hasRan()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        init();
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        Item item1 = subscriptionHandler.getSubscribedItem("anItemTemplate");

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        Item item2 = subscriptionHandler.getSubscribedItem("anotherItemTemplate");

        Item removed1 = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);

        // The consumer is still alive
        // assertThat(kafkaConsumer.isClosed()).isFalse();
        assertThat(subscriptionHandler.isConsuming()).isTrue();

        Item removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");
        assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(0);
        assertThat(removed1).isSameInstanceAs(item1);
        assertThat(removed2).isSameInstanceAs(item2);
        // The consumer stops only when no more subscriptions exist.
        // assertThat(kafkaConsumer.isClosed()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldNotUnsubscribe() {
        init();
        assertThrows(
                SubscriptionException.class,
                () -> subscriptionHandler.unsubscribe("anItemTemplate"));
    }
}
