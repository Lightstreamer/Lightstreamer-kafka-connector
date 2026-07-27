
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

package com.lightstreamer.kafka.adapters;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.ForceableSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.OnDemandSubscriptionsHandler;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

class KafkaConnectorDataAdapterTest {

    private Path adapterDir;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
    }

    @AfterEach
    void after() throws IOException {
        FileUtils.deleteDirectory(adapterDir.toFile());
    }

    @Test
    void shouldCreateKafkaConsumerFromDefaultFactory() {
        Function<Properties, Consumer<byte[], byte[]>> defaultConsumerFactory =
                KafkaConnectorDataAdapter.defaultConsumerFactory(
                        LoggerFactory.getLogger("TestConnection"));
        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        ConnectorConfigurator configurator =
                new ConnectorConfigurator(minimalConfig, adapterDir.toFile());

        KafkaException ke =
                assertThrows(
                        KafkaException.class,
                        () ->
                                defaultConsumerFactory.apply(
                                        configurator.connectionSpec().consumerProperties()));
        assertThat(ke).hasMessageThat().isEqualTo("Failed to construct kafka consumer");
    }

    @Test
    void shouldInitWithOnDemandSubscriptionsHandler() throws DataProviderException {
        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        minimalConfig.put(ConnectorConfig.DATA_ADAPTER_NAME, "TEST-CONNECTOR");

        connectorDataAdapter.init(minimalConfig, adapterDir.toFile());
        assertThat(connectorDataAdapter.getLogger().getName()).isEqualTo("TEST-CONNECTOR");

        SubscriptionsHandler<?, ?> subscriptionsHandler =
                connectorDataAdapter.getSubscriptionsHandler();
        assertThat(subscriptionsHandler).isNotNull();
        assertThat(subscriptionsHandler).isInstanceOf(OnDemandSubscriptionsHandler.class);
    }

    @Test
    void shouldInitWithForceableSubscriptionsHandler() throws DataProviderException {
        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        minimalConfig.put(ConnectorConfig.DATA_ADAPTER_NAME, "TEST-CONNECTOR");
        minimalConfig.put(ConnectorConfig.ITEM_SNAPSHOT_ENABLED_MODE, "MERGE");
        minimalConfig.put(ConnectorConfig.ITEM_SNAPSHOT_MAX_IDLE_SECONDS, "160");

        connectorDataAdapter.init(minimalConfig, adapterDir.toFile());
        assertThat(connectorDataAdapter.getLogger().getName()).isEqualTo("TEST-CONNECTOR");

        SubscriptionsHandler<?, ?> subscriptionsHandler =
                connectorDataAdapter.getSubscriptionsHandler();
        assertThat(subscriptionsHandler).isNotNull();
        assertThat(subscriptionsHandler).isInstanceOf(ForceableSubscriptionsHandler.class);
        assertThat(
                        ((ForceableSubscriptionsHandler<?, ?>) subscriptionsHandler)
                                .getItemSnapshotMaxIdleSeconds())
                .isEqualTo(160);
    }

    @Test
    void shouldSetListener() throws DataProviderException {
        AtomicReference<ItemEventListener> receivedListener = new AtomicReference<>(null);

        Supplier<SubscriptionsHandler<?, ?>> subscriptionHandlerSupplier =
                () -> new SubscriptionHandlerTestImpl<>(receivedListener::set, null);

        KafkaConnectorDataAdapterImplTest connectorDataAdapter =
                new KafkaConnectorDataAdapterImplTest(subscriptionHandlerSupplier);

        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        minimalConfig.put(ConnectorConfig.DATA_ADAPTER_NAME, "TEST-CONNECTOR");
        connectorDataAdapter.init(minimalConfig, adapterDir.toFile());

        SubscriptionsHandler<?, ?> subscriptionsHandler =
                connectorDataAdapter.getSubscriptionsHandler();
        assertThat(subscriptionsHandler).isNotNull();
        assertThat(subscriptionsHandler).isInstanceOf(SubscriptionHandlerTestImpl.class);

        MockItemEventListener eventListener = new MockItemEventListener();
        connectorDataAdapter.setListener(eventListener);
        assertThat(receivedListener.get()).isSameInstanceAs(eventListener);
    }

    @Test
    void shouldSubscribe() throws Exception {
        AtomicReference<String> receivedItemName = new AtomicReference<>(null);
        AtomicReference<Object> receivedHandle = new AtomicReference<>(null);

        BiConsumer<String, Object> receivedSubscription =
                new BiConsumer<String, Object>() {
                    @Override
                    public void accept(String itemName, Object itemHandle) {
                        receivedItemName.set(itemName);
                        receivedHandle.set(itemHandle);
                    }
                };

        Supplier<SubscriptionsHandler<?, ?>> subscriptionHandlerSupplier =
                () -> new SubscriptionHandlerTestImpl<>(receivedSubscription);

        KafkaConnectorDataAdapterImplTest connectorDataAdapter =
                new KafkaConnectorDataAdapterImplTest(subscriptionHandlerSupplier);

        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        minimalConfig.put(ConnectorConfig.DATA_ADAPTER_NAME, "TEST-CONNECTOR");
        connectorDataAdapter.init(minimalConfig, adapterDir.toFile());

        SubscriptionsHandler<?, ?> subscriptionsHandler =
                connectorDataAdapter.getSubscriptionsHandler();
        assertThat(subscriptionsHandler).isNotNull();
        assertThat(subscriptionsHandler).isInstanceOf(SubscriptionHandlerTestImpl.class);

        connectorDataAdapter.subscribe("anItemTemplate", new Object(), false);
        assertThat(receivedItemName.get()).isEqualTo("anItemTemplate");
        assertThat(receivedHandle.get()).isNotNull();
    }

    @Test
    void shouldUnsubscribe() throws Exception {
        AtomicReference<String> receivedItemName = new AtomicReference<>(null);

        Supplier<SubscriptionsHandler<?, ?>> subscriptionHandlerSupplier =
                () -> new SubscriptionHandlerTestImpl<>(null, receivedItemName::set);

        KafkaConnectorDataAdapterImplTest connectorDataAdapter =
                new KafkaConnectorDataAdapterImplTest(subscriptionHandlerSupplier);

        Map<String, String> minimalConfig = ConnectorConfigProvider.minimalConfig();
        minimalConfig.put(ConnectorConfig.DATA_ADAPTER_NAME, "TEST-CONNECTOR");
        connectorDataAdapter.init(minimalConfig, adapterDir.toFile());

        SubscriptionsHandler<?, ?> subscriptionsHandler =
                connectorDataAdapter.getSubscriptionsHandler();
        assertThat(subscriptionsHandler).isNotNull();
        assertThat(subscriptionsHandler).isInstanceOf(SubscriptionHandlerTestImpl.class);

        connectorDataAdapter.unsubscribe("anItemTemplate");
        assertThat(receivedItemName.get()).isEqualTo("anItemTemplate");
    }

    @Test
    void shouldNotHandleSnapshot() throws Exception {
        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        connectorDataAdapter.setConsumerFactory(this.getConsumer());
        connectorDataAdapter.init(ConnectorConfigProvider.minimalConfig(), adapterDir.toFile());
        connectorDataAdapter.setListener(new MockItemEventListener());

        assertThat(connectorDataAdapter.isSnapshotAvailable("anItem")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMMAND", "MERGE", "DISTINCT"})
    void shouldHandleSnapshot(String mode) throws Exception {
        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        connectorDataAdapter.setConsumerFactory(this.getConsumer());
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.ITEM_SNAPSHOT_ENABLED_MODE, mode);
        if (mode.equals("COMMAND")) {
            config.put("field.key", "#{KEY}");
        }
        connectorDataAdapter.init(
                ConnectorConfigProvider.minimalConfigWith(config), adapterDir.toFile());
        // Here we don't call setListener because snapshot availability should not depend on it, but
        // rather on the configuration only
        assertThat(connectorDataAdapter.isSnapshotAvailable("anItem")).isTrue();
    }

    private Function<Properties, Consumer<byte[], byte[]>> getConsumer() {
        MockConsumer consumer = new Mocks.MockConsumer(StrategyType.LATEST.toString());

        TopicPartition tp = new TopicPartition("topic", 0);
        consumer.updatePartitions(
                tp.topic(),
                List.of(new PartitionInfo(tp.topic(), tp.partition(), null, null, null)));
        return props -> consumer;
    }

    static class KafkaConnectorDataAdapterImplTest extends KafkaConnectorDataAdapter {

        private final Supplier<SubscriptionsHandler<?, ?>> supplier;

        KafkaConnectorDataAdapterImplTest(
                Supplier<SubscriptionsHandler<?, ?>> subscriptionsHandlerSupplier) {
            this.supplier = subscriptionsHandlerSupplier;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <K, V> SubscriptionsHandler<K, V> subscriptionHandler(
                ConnectionSpec<K, V> connectionSpec) throws DataProviderException {
            return (SubscriptionsHandler<K, V>) supplier.get();
        }
    }

    static class SubscriptionHandlerTestImpl<K, V> implements SubscriptionsHandler<K, V> {

        private java.util.function.Consumer<ItemEventListener> setListenerCallbackConsumer;
        private BiConsumer<String, Object> subscribeCallbackConsumer;
        private java.util.function.Consumer<String> unsubscribeCallbackConsumer;

        SubscriptionHandlerTestImpl(
                java.util.function.Consumer<ItemEventListener> setListenerCallbackConsumer,
                BiConsumer<String, Object> subscribeCallbackConsumer,
                java.util.function.Consumer<String> unsubscribeCallbackConsumer) {
            this.setListenerCallbackConsumer = setListenerCallbackConsumer;
            this.subscribeCallbackConsumer = subscribeCallbackConsumer;
            this.unsubscribeCallbackConsumer = unsubscribeCallbackConsumer;
        }

        SubscriptionHandlerTestImpl(
                java.util.function.Consumer<ItemEventListener> setListenerCallbackConsumer,
                java.util.function.Consumer<String> unsubscribeCallbackConsumer) {
            this(setListenerCallbackConsumer, null, unsubscribeCallbackConsumer);
        }

        SubscriptionHandlerTestImpl(BiConsumer<String, Object> subscribeCallbackConsumer) {
            this(null, subscribeCallbackConsumer, null);
        }

        @Override
        public boolean isSnapshotAvailable(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'isSnapshotAvailable'");
        }

        @Override
        public boolean unsubscribe(String item) {
            if (unsubscribeCallbackConsumer != null) {
                unsubscribeCallbackConsumer.accept(item);
            }
            return true;
        }

        @Override
        public void setListener(ItemEventListener listener) {
            if (setListenerCallbackConsumer != null) {
                setListenerCallbackConsumer.accept(listener);
            }
        }

        @Override
        public void subscribe(String item, Object itemHandle) throws SubscriptionException {
            if (subscribeCallbackConsumer != null) {
                subscribeCallbackConsumer.accept(item, itemHandle);
            }
        }
    }
}
