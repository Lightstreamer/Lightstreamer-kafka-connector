
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

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SmartDataProvider;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ItemSnapshotEnabledMode;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.KafkaConnectorDataAdapterOpts;
import com.lightstreamer.kafka.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * {@link SmartDataProvider} implementation that bridges Lightstreamer subscriptions to Kafka
 * consumers.
 *
 * <p>On {@link #init(Map, File)}, the adapter configures a {@link SubscriptionsHandler} that
 * manages the Kafka consumer lifecycle based on the configured snapshot mode.
 */
public class KafkaConnectorDataAdapter implements SmartDataProvider {

    private Logger logger;
    private SubscriptionsHandler<?, ?> subscriptionsHandler;
    private ConnectorConfig connectorConfig;
    private MetadataListener metadataListener;
    private Function<Properties, Consumer<byte[], byte[]>> consumerFactory;

    public KafkaConnectorDataAdapter() {}

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void init(@Nonnull Map params, @Nonnull File configDir) throws DataProviderException {
        ConnectorConfigurator configurator = new ConnectorConfigurator(params, configDir);
        connectorConfig = configurator.getConfig();
        logger = LogFactory.getLogger(connectorConfig.getAdapterName());
        metadataListener =
                KafkaConnectorMetadataAdapter.listener(
                        new KafkaConnectorDataAdapterOpts(
                                connectorConfig.getAdapterName(),
                                connectorConfig.isEnabled(),
                                connectorConfig.getSubscriptionMode(),
                                connectorConfig.getItemSnapshotDistinctLength()));

        logger.atInfo().log("Configuring Kafka Connector");
        subscriptionsHandler = subscriptionHandler(configurator.connectionSpec());
        logger.atInfo().log("KafkaConnector configuration complete");
    }

    /**
     * Builds a {@link SubscriptionsHandler} for the given connection specification.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     * @param connectionSpec the {@link ConnectionSpec} defining consumer settings
     * @return a configured {@code SubscriptionsHandler}
     * @throws DataProviderException if the handler cannot be built
     */
    protected <K, V> SubscriptionsHandler<K, V> subscriptionHandler(
            ConnectionSpec<K, V> connectionSpec) throws DataProviderException {
        return SubscriptionsHandler.<K, V>builder()
                .consumerFactory(
                        Objects.requireNonNullElse(consumerFactory, defaultConsumerFactory(logger)))
                .connectionSpec(connectionSpec)
                .metadataListener(metadataListener)
                .snapshotEnabled(
                        !connectorConfig.getItemSnapshotMode().equals(ItemSnapshotEnabledMode.NONE))
                .itemSnapshotMaxIdleSeconds(connectorConfig.getItemSnapshotMaxIdleSeconds())
                .build();
    }

    /**
     * Injects a custom consumer factory.
     *
     * @param consumerFactory function that creates a Kafka consumer from the given properties
     */
    @VisibleForTesting
    public final void setConsumerFactory(
            Function<Properties, Consumer<byte[], byte[]>> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    static <K, V> Function<Properties, Consumer<byte[], byte[]>> defaultConsumerFactory(
            Logger logger) {
        return props -> {
            ConsumerConfig cfg = new ConsumerConfig(props);
            String configString =
                    cfg.values().entrySet().stream()
                            .map(e -> e.getKey() + " = " + e.getValue())
                            .sorted()
                            .collect(Collectors.joining("\n\t"));
            logger.atDebug().log("Kafka consumer configuration:\n\t{}", configString);
            return new KafkaConsumer<>(cfg.originals());
        };
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return subscriptionsHandler.isSnapshotAvailable(itemName);
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        subscriptionsHandler.setListener(eventListener);
    }

    @Override
    public void subscribe(@Nonnull String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {}

    @Override
    public void subscribe(
            @Nonnull String itemName, @Nonnull Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        subscriptionsHandler.subscribe(itemName, itemHandle);
    }

    @Override
    public void unsubscribe(@Nonnull String itemName)
            throws SubscriptionException, FailureException {
        subscriptionsHandler.unsubscribe(itemName);
    }

    @VisibleForTesting
    Logger getLogger() {
        return logger;
    }

    @VisibleForTesting
    SubscriptionsHandler<?, ?> getSubscriptionsHandler() {
        return subscriptionsHandler;
    }
}
