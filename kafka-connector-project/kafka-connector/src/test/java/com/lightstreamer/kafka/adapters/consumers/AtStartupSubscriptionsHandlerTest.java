
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
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class AtStartupSubscriptionsHandlerTest {

    private static AtStartupSubscriptionsHandler<String, String> mkSubscriptionsHandler(
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
                        OthersSelectorSuppliers.String().deserializers(),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        CommandModeStrategy.NONE,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        Mocks.MockConsumer<String, String> consumer =
                new Mocks.MockConsumer<>(OffsetResetStrategy.EARLIEST);
        for (String topic : topics) {
            consumer.updatePartitions(
                    topic, List.of(new PartitionInfo(topic, 0, null, null, null)));
        }

        Supplier<Consumer<String, String>> supplier =
                () -> {
                    if (exceptionOnConnection) {
                        throw new KafkaException("Simulated Exception");
                    }
                    return consumer;
                };

        return new AtStartupSubscriptionsHandler<>(
                config, new Mocks.MockMetadataListener(), false, supplier);
    }

    private AtStartupSubscriptionsHandler<String, String> subscriptionHandler;

    void init(String... topics) {
        init(false, topics);
    }

    void init(boolean exceptionOnConnection, String... topics) {
        subscriptionHandler = mkSubscriptionsHandler(exceptionOnConnection, topics);
    }

    @Test
    public void shouldInit() throws InterruptedException {
        init("aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        assertThat(listener.getFailure()).isNull();
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    @Test
    public void shouldFailOnInitDueToNonExistingTopic() {
        init("notExistingTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        Throwable failure = listener.getFailure();
        assertThat(failure).isNotNull();
        assertThat(failure).hasMessageThat().isEqualTo("Failed to start consuming from Kafka");
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldFailOnInitDueToExceptionWhileConnecting() {
        init(true, "aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        Throwable failure = listener.getFailure();
        assertThat(failure).isNotNull();
        assertThat(failure).isInstanceOf(KafkaException.class);
        assertThat(subscriptionHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        init("aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);

        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        // assertThat(subscriptionHandler.isConsuming()).isFalse();

        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        // assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(1);
        // assertThat(subscriptionHandler.isConsuming()).isTrue();

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        // assertThat(subscriptionHandler.getItemsCounter()).isEqualTo(2);
        // assertThat(subscriptionHandler.isConsuming()).isTrue();

        Item item1 = subscriptionHandler.getSubscribedItem("anItemTemplate");
        assertThat(item1).isEqualTo(Items.subscribedFrom("anItemTemplate", itemHandle1));

        Item item2 = subscriptionHandler.getSubscribedItem("anotherItemTemplate");
        assertThat(item2).isEqualTo(Items.subscribedFrom("anotherItemTemplate", itemHandle2));
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init("aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");
    }

    @Test
    public void shouldFailSubscriptionDueInvalidExpression() {
        init("aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        init("aTopic");
        Mocks.MockItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionHandler.setListener(listener);
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        subscriptionHandler.subscribe("anItemTemplate", itemHandle1);
        Item item1 = subscriptionHandler.getSubscribedItem("anItemTemplate");

        subscriptionHandler.subscribe("anotherItemTemplate", itemHandle2);
        Item item2 = subscriptionHandler.getSubscribedItem("anotherItemTemplate");

        Item removed1 = subscriptionHandler.unsubscribe("anItemTemplate");

        Item removed2 = subscriptionHandler.unsubscribe("anotherItemTemplate");
        assertThat(removed1).isSameInstanceAs(item1);
        assertThat(removed2).isSameInstanceAs(item2);

        // Even after unsubscription, the handler should still be consuming
        assertThat(subscriptionHandler.isConsuming()).isTrue();
    }

    // @Test
    // public void shouldNotUnsubscribe() {
    //     init(new Mocks.MockItemEventListener());
    //     assertThrows(
    //             SubscriptionException.class,
    //             () -> subscriptionHandler.unsubscribe("anItemTemplate"));
    // }
}
