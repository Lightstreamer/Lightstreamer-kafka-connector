
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
import static com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig.fromDelimitedMappings;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.Fakes.FakeKafkaConsumer;
import com.lightstreamer.kafka.adapters.consumers.Fakes.FakeMetadataListener;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class TestLoopConfig implements ConsumerTriggerConfig<String, String> {

    private final TopicConfigurations topicsConfig;

    TestLoopConfig(TopicConfigurations topicsConfig) {
        this.topicsConfig = topicsConfig;
    }

    @Override
    public Properties consumerProperties() {
        return new Properties();
    }

    @Override
    public DataExtractor<String, String> fieldsExtractor() {
        throw new UnsupportedOperationException("Unimplemented method 'fieldMappings'");
    }

    @Override
    public ItemTemplates<String, String> itemTemplates() {
        try {
            return Items.templatesFrom(topicsConfig, OthersSelectorSuppliers.String());
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String connectionName() {
        return "TestConnection";
    }

    @Override
    public KeyValueDeserializers<String, String> deserializers() {
        return null;
    }

    @Override
    public Concurrency concurrency() {
        throw new UnsupportedOperationException("Unimplemented method 'recordConsumption'");
    }

    @Override
    public RecordErrorHandlingStrategy errorHandlingStrategy() {
        return RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
    }
}

public class ConsumerTriggerTest {

    private static ConsumerTriggerImpl<?, ?> mkConsumerTrigger(
            MetadataListener metadataListener,
            ConsumerWrapper<String, String> consumer,
            boolean throwExceptionWhileConnectingToKafka) {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(),
                        List.of(
                                fromDelimitedMappings(
                                        "aTopic", "anItemTemplate,anotherItemTemplate")));
        ConsumerTriggerConfig<String, String> config = new TestLoopConfig(topicsConfig);

        Function<Collection<SubscribedItem>, ConsumerWrapper<String, String>> consumerWrapper =
                items -> {
                    if (throwExceptionWhileConnectingToKafka) {
                        throw new KafkaException("Simulated Exception");
                    }
                    return consumer;
                };
        return new ConsumerTriggerImpl<>(config, metadataListener, consumerWrapper);
    }

    private ConsumerTriggerImpl<?, ?> consumerTrigger;
    private FakeMetadataListener metadataListener;
    private FakeKafkaConsumer<String, String> kafkaConsumer;

    void init() {
        init(false);
    }

    void init(boolean throwExceptionWhileConnectingToKafka) {
        kafkaConsumer = new Fakes.FakeKafkaConsumer<>();
        metadataListener = new Fakes.FakeMetadataListener();
        consumerTrigger =
                mkConsumerTrigger(
                        metadataListener, kafkaConsumer, throwExceptionWhileConnectingToKafka);
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(0);
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        init();
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();

        CompletableFuture<Void> consuming1 =
                consumerTrigger.subscribe("anItemTemplate", itemHandle1);
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(1);

        CompletableFuture<Void> consuming2 =
                consumerTrigger.subscribe("anotherItemTemplate", itemHandle2);
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(2);

        Item item1 = consumerTrigger.getSubscribeditem("anItemTemplate");
        assertThat(item1).isEqualTo(Items.subcribedFrom("anItemTemplate", itemHandle1));

        Item item2 = consumerTrigger.getSubscribeditem("anotherItemTemplate");
        assertThat(item2).isEqualTo(Items.subcribedFrom("anotherItemTemplate", itemHandle2));

        assertThat(consuming1).isSameInstanceAs(consuming2);

        consuming1.join();
        assertThat(kafkaConsumer.hasRan()).isTrue();
        assertThat(kafkaConsumer.isClosed()).isFalse();
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        init();
        Object itemHandle = new Object();
        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> consumerTrigger.subscribe("@invalidItem@", itemHandle));
        assertThat(se.getMessage()).isEqualTo("Invalid Item");
    }

    @Test
    public void shouldFailSubscriptionDueInvalidExpression() {
        init();
        Object itemHandle = new Object();
        assertThrows(
                SubscriptionException.class,
                () -> consumerTrigger.subscribe("unregisteredTemplate", itemHandle));
    }

    @Test
    public void shouldFailSubscription() throws SubscriptionException {
        init(true);
        Object itemHandle = new Object();

        CompletableFuture<Void> consuming = consumerTrigger.subscribe("anItemTemplate", itemHandle);
        Item item = consumerTrigger.getSubscribeditem("anItemTemplate");

        assertThat(item).isEqualTo(Items.subcribedFrom("anItemTemplate", itemHandle));
        assertThat(consuming.isCompletedExceptionally());
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(1);
        assertThat(kafkaConsumer.hasRan()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        init();
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        consumerTrigger.subscribe("anItemTemplate", itemHandle1);
        Item item1 = consumerTrigger.getSubscribeditem("anItemTemplate");

        consumerTrigger.subscribe("anotherItemTemplate", itemHandle2);
        Item item2 = consumerTrigger.getSubscribeditem("anotherItemTemplate");

        Item removed1 = consumerTrigger.unsubscribe("anItemTemplate");
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(1);

        // The consumer is still alive
        assertThat(kafkaConsumer.isClosed()).isFalse();

        Item removed2 = consumerTrigger.unsubscribe("anotherItemTemplate");
        assertThat(consumerTrigger.getItemsCounter()).isEqualTo(0);
        assertThat(removed1).isSameInstanceAs(item1);
        assertThat(removed2).isSameInstanceAs(item2);
        // The consumer stops only when no more subscriptions exist.
        assertThat(kafkaConsumer.isClosed()).isTrue();
    }

    @Test
    public void shouldNotUnsubscribe() {
        init();
        assertThrows(
                SubscriptionException.class, () -> consumerTrigger.unsubscribe("anItemTemplate"));
    }
}
