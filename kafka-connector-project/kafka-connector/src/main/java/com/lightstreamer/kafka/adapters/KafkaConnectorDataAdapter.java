
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

import static com.lightstreamer.kafka.adapters.consumers.snapshot.SnapshotDeliveryStrategy.SnapshotMode.*;

import static org.apache.kafka.common.serialization.Serdes.ByteArray;

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SmartDataProvider;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.KafkaConnectorDataAdapterOpts;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

public class KafkaConnectorDataAdapter implements SmartDataProvider {

    private static final Deserializer<byte[]> BYTE_ARRAY_DESERIALIZER = ByteArray().deserializer();

    private Logger logger;
    private SubscriptionsHandler<?, ?> subscriptionsHandler;
    private ConnectorConfig connectorConfig;
    private MetadataListener metadataListener;
    private Supplier<Consumer<byte[], byte[]>> consumerSupplier;
    private Supplier<Consumer<byte[], byte[]>> snapshotConsumerSupplier;

    public KafkaConnectorDataAdapter() {}

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Map params, @Nonnull File configDir) throws DataProviderException {
        ConnectorConfigurator configurator = new ConnectorConfigurator(params, configDir);
        this.connectorConfig = configurator.getConfig();
        this.logger = LogFactory.getLogger(connectorConfig.getAdapterName());
        this.metadataListener =
                KafkaConnectorMetadataAdapter.listener(
                        new KafkaConnectorDataAdapterOpts(
                                connectorConfig.getAdapterName(),
                                connectorConfig.isEnabled(),
                                connectorConfig.isAutoCommandModeEnabled()
                                        || connectorConfig.isCommandEnforceEnabled()));

        this.logger.info("Configuring Kafka Connector");
        this.subscriptionsHandler = subscriptionHandler(configurator.connectionSpec());
        this.logger.info("KafkaConnector configuration complete");
    }

    protected <K, V> SubscriptionsHandler<K, V> subscriptionHandler(
            ConnectionSpec<K, V> connectionSpec) throws DataProviderException {
        return SubscriptionsHandler.<K, V>builder()
                .withConnectionSpec(connectionSpec)
                .withMetadataListener(metadataListener)
                .withConsumerSupplier(
                        Objects.requireNonNullElse(
                                consumerSupplier,
                                consumerSupplier(connectionSpec.consumerProperties())))
                // .withSnapshotMode(
                //         DISABLED) // TODO: gate on a snapshot.enable configuration property
                .withSnapshotMode(CACHE)
                // .withSnapshotMode(IMPLICIT_ITEM_SNAPSHOT)
                .withSnapshotConsumerSupplier(
                        Objects.requireNonNullElse(
                                snapshotConsumerSupplier, snapshotConsumerSupplier(connectionSpec)))
                .build();
    }

    public void setSnapshotConsumerSupplier(
            Supplier<org.apache.kafka.clients.consumer.Consumer<byte[], byte[]>>
                    snapshotConsumerSupplier) {
        this.snapshotConsumerSupplier = snapshotConsumerSupplier;
    }

    public void setConsumerSupplier(
            Supplier<org.apache.kafka.clients.consumer.Consumer<byte[], byte[]>> consumerSupplier) {
        this.consumerSupplier = consumerSupplier;
    }

    private static <K, V> Supplier<Consumer<byte[], byte[]>> snapshotConsumerSupplier(
            ConnectionSpec<K, V> connectionSpec) {
        Properties snapshotProps = new Properties();
        snapshotProps.putAll(connectionSpec.consumerProperties());
        snapshotProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        return consumerSupplier(snapshotProps);
    }

    private static <K, V> Supplier<Consumer<byte[], byte[]>> consumerSupplier(
            Properties consumerProps) {
        return () ->
                new KafkaConsumer<>(
                        consumerProps, BYTE_ARRAY_DESERIALIZER, BYTE_ARRAY_DESERIALIZER);
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        // TODO: temporarily always enabled; gate on a snapshot.enable configuration property
        return true;
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        this.subscriptionsHandler.setListener(eventListener);
    }

    @Override
    public void subscribe(@Nonnull String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {}

    @Override
    public void subscribe(
            @Nonnull String itemName, @Nonnull Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        // logger.info("Trying subscription to item [{}]", itemName);
        subscriptionsHandler.subscribe(itemName, itemHandle);
    }

    @Override
    public void unsubscribe(@Nonnull String itemName)
            throws SubscriptionException, FailureException {
        // logger.info("Unsubscribing from item [{}]", itemName);
        subscriptionsHandler.unsubscribe(itemName);
    }
}
