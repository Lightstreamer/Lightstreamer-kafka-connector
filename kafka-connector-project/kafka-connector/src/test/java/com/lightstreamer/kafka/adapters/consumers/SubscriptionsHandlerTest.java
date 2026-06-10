
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

import static org.junit.Assert.assertThrows;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluateCommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.AbstractSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.Builder;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.OnDemandSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SubscriptionsHandlerTest {

    @Test
    public void shouldNotBuildSubscriptionsHandler() {
        IllegalStateException ise =
                assertThrows(
                        IllegalStateException.class,
                        () -> SubscriptionsHandler.<String, String>builder().build());
        assertThat(ise).hasMessageThat().isEqualTo("ConsumerFactory not set");

        ise =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SubscriptionsHandler.<String, String>builder()
                                        .withConsumerFactory(MockConsumer.factory())
                                        .build());
        assertThat(ise).hasMessageThat().isEqualTo("ConnectionSpec not set");

        ise =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SubscriptionsHandler.<String, String>builder()
                                        .withConnectionSpec(
                                                makeConnectionSpec(EvaluateCommandMode.EXPLICIT))
                                        .withConsumerFactory(MockConsumer.factory())
                                        .withItemSnapshotEnabled(true)
                                        .build());
        assertThat(ise)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid configuration: command mode EXPLICIT is not compatible with item snapshot enablement");
        ise =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SubscriptionsHandler.<String, String>builder()
                                        .withConnectionSpec(makeConnectionSpec())
                                        .withConsumerFactory(MockConsumer.factory())
                                        .build());

        assertThat(ise).hasMessageThat().isEqualTo("MetadataListener not set");
    }

    @ParameterizedTest
    @EnumSource(EvaluateCommandMode.class)
    public void shouldBuildOnDemandSubscriptionsHandlerWhenSnapshotModeDisabled(
            EvaluateCommandMode commandMode) {
        SubscriptionsHandler<String, String> subscriptionsHandler =
                builder(commandMode).withMetadataListener(new Mocks.MockMetadataListener()).build();
        assertThat(subscriptionsHandler)
                .isInstanceOf(SubscriptionsHandler.OnDemandSubscriptionsHandler.class);
        assertThat(
                        ((OnDemandSubscriptionsHandler<String, String>) subscriptionsHandler)
                                .isConsuming())
                .isFalse();

        subscriptionsHandler =
                builder(commandMode)
                        .withMetadataListener(new Mocks.MockMetadataListener())
                        .withItemSnapshotEnabled(false)
                        .build();
        assertThat(subscriptionsHandler)
                .isInstanceOf(SubscriptionsHandler.OnDemandSubscriptionsHandler.class);
        assertThat(
                        ((OnDemandSubscriptionsHandler<String, String>) subscriptionsHandler)
                                .isConsuming())
                .isFalse();
    }

    @ParameterizedTest
    @EnumSource(
            value = EvaluateCommandMode.class,
            names = {"DISABLED", "AUTO"},
            mode = EnumSource.Mode.INCLUDE)
    public void shouldBuildForceableSubscriptionsHandlerWhenSnapshotModeEnabled(
            EvaluateCommandMode commandMode) {
        SubscriptionsHandler<String, String> subscriptionsHandler =
                builder(commandMode).withItemSnapshotEnabled(true).build();
        assertThat(subscriptionsHandler)
                .isInstanceOf(SubscriptionsHandler.ForceableSubscriptionsHandler.class);
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        AtomicReference<SubscriptionExpression> receivedExpression = new AtomicReference<>(null);
        AtomicReference<Object> receivedHandle = new AtomicReference<>(null);

        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(
                        builder(),
                        (se, handle) -> {
                            receivedExpression.set(se);
                            receivedHandle.set(handle);
                        });

        Object itemHandle = new Object();
        subscriptionsHandler.subscribe("anItemTemplate", itemHandle);
        assertThat(receivedExpression.get().canonicalItemName()).isEqualTo("anItemTemplate");
        assertThat(receivedHandle.get()).isEqualTo(itemHandle);
    }

    @Test
    public void shouldSetListener() {
        AtomicReference<ItemEventListener> receivedListener = new AtomicReference<>(null);

        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(
                        builder(), listener -> receivedListener.set(listener), null);

        ItemEventListener listener = new Mocks.MockItemEventListener();
        subscriptionsHandler.setListener(listener);
        assertThat(receivedListener.get()).isEqualTo(listener);
    }

    @Test
    public void shouldRejectNullListener() {
        AtomicReference<ItemEventListener> receivedListener = new AtomicReference<>(null);

        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(
                        builder(), listener -> receivedListener.set(listener), null);

        IllegalArgumentException iae =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> subscriptionsHandler.setListener(null));
        assertThat(iae).hasMessageThat().isEqualTo("ItemEventListener cannot be null");
        assertThat(receivedListener.get()).isNull();
    }

    @Test
    public void shouldFailCreateNewConsumer() {
        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(builder());

        RuntimeException re =
                assertThrows(
                        RuntimeException.class,
                        () -> subscriptionsHandler.newConsumer(true, SubscribedItems.onDemand()));
        assertThat(re)
                .hasMessageThat()
                .isEqualTo("ItemEventListener must be set before starting the consumer");
    }

    @Test
    public void shouldCreateNewConsumer() {
        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(builder());
        subscriptionsHandler.setListener(new Mocks.MockItemEventListener());

        KafkaConsumerWrapper<String, String> consumer =
                subscriptionsHandler.newConsumer(true, SubscribedItems.onDemand());
        assertThat(consumer).isNotNull();
    }

    @Test
    public void shouldFailSubscriptionDueToNotRegisteredTemplate() {
        AtomicBoolean subscribeCallbackInvoked = new AtomicBoolean(false);

        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(
                        builder(), (se, handle) -> subscribeCallbackInvoked.set(true));

        Object itemHandle = new Object();

        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionsHandler.subscribe("unregisteredTemplate", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Item does not match any defined item templates");

        // Verify that the subscription callback was not invoked, as the subscription should have
        // failed before reaching the point of invoking the callback.
        assertThat(subscribeCallbackInvoked.get()).isFalse();
    }

    @Test
    public void shouldFailSubscriptionDueToInvalidExpression() {
        AtomicBoolean subscribeCallbackInvoked = new AtomicBoolean(false);

        TestSubscriptionsHandler<String, String> subscriptionsHandler =
                new TestSubscriptionsHandler<>(
                        builder(), (se, handle) -> subscribeCallbackInvoked.set(true));

        Object itemHandle = new Object();

        SubscriptionException se =
                assertThrows(
                        SubscriptionException.class,
                        () -> subscriptionsHandler.subscribe("@invalidItem@", itemHandle));
        assertThat(se).hasMessageThat().isEqualTo("Invalid Item");

        // Verify that the subscription callback was not invoked, as the subscription should have
        // failed before reaching the point of invoking the callback.
        assertThat(subscribeCallbackInvoked.get()).isFalse();
    }

    private Builder<String, String> builder() {
        return builder(EvaluateCommandMode.DISABLED);
    }

    private Builder<String, String> builder(EvaluateCommandMode commandMode) {
        return SubscriptionsHandler.<String, String>builder()
                .withConnectionSpec(makeConnectionSpec(commandMode))
                .withConsumerFactory(MockConsumer.factory());
    }

    private static ConnectionSpec<String, String> makeConnectionSpec() {
        return makeConnectionSpec(EvaluateCommandMode.DISABLED);
    }

    private static ConnectionSpec<String, String> makeConnectionSpec(
            EvaluateCommandMode commandMode) {
        return new ConnectionSpec<>(
                "TestConnection",
                new Properties(),
                ItemTemplatesUtils.itemTemplates("aTopic", "anItemTemplate,anotherItemTemplate"),
                ItemTemplatesUtils.fieldsExtractor(),
                new KafkaRecord.DeserializerPair<>(
                        OthersSelectorSuppliers.String().keySelectorSupplier().deserializer(),
                        OthersSelectorSuppliers.String().valueSelectorSupplier().deserializer()),
                RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                commandMode,
                new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));
    }

    static class TestSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        private final Consumer<ItemEventListener> setListenerCallback;
        private final BiConsumer<SubscriptionExpression, Object> subscribeCallback;

        TestSubscriptionsHandler(
                Builder<K, V> builder,
                Consumer<ItemEventListener> setListenerCallback,
                BiConsumer<SubscriptionExpression, Object> subscribeCallback) {
            super(builder);
            this.subscribeCallback = subscribeCallback;
            this.setListenerCallback = setListenerCallback;
        }

        TestSubscriptionsHandler(
                Builder<K, V> builder,
                BiConsumer<SubscriptionExpression, Object> subscribeCallback) {
            this(builder, null, subscribeCallback);
        }

        TestSubscriptionsHandler(Builder<K, V> builder) {
            this(builder, null, null);
        }

        @Override
        public boolean unsubscribe(String item) {
            return false;
        }

        @Override
        public boolean isSnapshotAvailable(String itemName) {
            return false;
        }

        @Override
        void doSubscribe(SubscriptionExpression expression, Object handle)
                throws SubscriptionException {
            if (subscribeCallback != null) {
                subscribeCallback.accept(expression, handle);
            }
        }

        @Override
        protected void doSetListener(ItemEventListener listener) {
            if (setListenerCallback != null) {
                setListenerCallback.accept(listener);
            }
        }
    }
}
