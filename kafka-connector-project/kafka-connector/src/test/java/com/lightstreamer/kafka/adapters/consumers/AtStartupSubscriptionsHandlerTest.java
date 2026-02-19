
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
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.Builder;
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

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class AtStartupSubscriptionsHandlerTest {

    private AtStartupSubscriptionsHandler<String, String> subscriptionHandler;
    private SubscribedItems subscribedItems;
    private MockItemEventListener listener;
    private Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private AtStartupSubscriptionsHandler<String, String> mkSubscriptionsHandler(
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

        MockConsumer consumer = new MockConsumer(StrategyType.EARLIEST.toString());
        for (String topic : topics) {
            consumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }

        Builder<String, String> builder =
                SubscriptionsHandler.<String, String>builder()
                        .withConsumerConfig(config)
                        .withMetadataListener(metadataListener)
                        .withConsumerSupplier(
                                () -> {
                                    if (exceptionOnConnection) {
                                        throw new KafkaException("Simulated Exception");
                                    }
                                    return consumer;
                                })
                        .atStartup(true);

        return new AtStartupSubscriptionsHandler<>(builder);
    }

    void init(String... topics) {
        init(false, topics);
    }

    void init(boolean exceptionOnConnection, String... topics) {
        this.subscriptionHandler = mkSubscriptionsHandler(exceptionOnConnection, topics);
        this.listener = new MockItemEventListener();
        this.subscriptionHandler.setListener(listener);
        this.subscribedItems = subscriptionHandler.getSubscribedItems();
    }

    @Test
    public void shouldInit() {
        init("aTopic");
        assertThat(listener.getFailures()).isEmpty();
        assertThat(subscriptionHandler.consumeAtStartup()).isTrue();
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    @Test
    public void shouldFailOnInitDueToExceptionWhileConnecting() {
        init(true, "aTopic");

        assertThat(listener.getFailures()).hasSize(1);
        Throwable failure = listener.getFailures().get(0);
        assertThat(failure).isNotNull();
        assertThat(failure).isInstanceOf(KafkaException.class);
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        init("aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
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
    public void shouldFailSubscriptionDueToInvalidExpression() {
        init("aTopic");

        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");
        assertThat(subscriptionHandler.isConsuming()).isTrue();
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldForceUnsubscriptionWhenSubscribeToNonExistingTopics() {
        init(false, "notExistingTopic");

        assertThat(listener.getFailures()).hasSize(1);
        Throwable failure = listener.getFailures().get(0);
        assertThat(failure).isNotNull();
        assertThat(failure).hasMessageThat().isEqualTo("Failed to start consuming from Kafka");
        assertThat(subscriptionHandler.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init("aTopic");

        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");
        assertThat(metadataListener.forcedUnsubscription()).isFalse();
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        init(false, "aTopic");

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        SubscribedItem item1 = subscribedItems.getItem("anItemTemplate");

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        SubscribedItem item2 = subscribedItems.getItem("anotherItemTemplate");

        Optional<SubscribedItem> removed1 = subscriptionHandler.unsubscribe("anItemTemplate");
        Optional<SubscribedItem> removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");

        assertThat(removed1.get()).isSameInstanceAs(item1);
        assertThat(removed2.get()).isSameInstanceAs(item2);

        // Even after unsubscription, the handler should still be consuming
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    @Test
    public void shouldNotUnsubscribeNonExistingItem() {
        init("aTopic");

        Optional<SubscribedItem> unsubscribed = subscriptionHandler.unsubscribe("anItemTemplate");
        assertThat(unsubscribed).isEmpty();
    }
}
