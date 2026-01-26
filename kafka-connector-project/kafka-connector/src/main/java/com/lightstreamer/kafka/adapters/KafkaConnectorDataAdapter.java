
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
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.KafkaConnectorDataAdapterOpts;

import org.slf4j.Logger;

import java.io.File;
import java.util.Map;

import javax.annotation.Nonnull;

public final class KafkaConnectorDataAdapter implements SmartDataProvider {

    private Logger logger;
    private SubscriptionsHandler<?, ?> subscriptionsHandler;
    private ConnectorConfig connectorConfig;
    private MetadataListener metadataListener;

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
        this.subscriptionsHandler = subscriptionHandler(configurator.consumerConfig());
        this.logger.info("KafkaConnector configuration complete");
    }

    private <K, V> SubscriptionsHandler<K, V> subscriptionHandler(Config<K, V> consumerConfig)
            throws DataProviderException {
        return SubscriptionsHandler.<K, V>builder()
                .withConsumerConfig(consumerConfig)
                .withMetadataListener(metadataListener)
                .build();
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return connectorConfig.getCommandModeStrategy().manageSnapshot();
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
        logger.info("Trying subscription to item [{}]", itemName);
        subscriptionsHandler.subscribe(itemName, itemHandle);
    }

    @Override
    public void unsubscribe(@Nonnull String itemName)
            throws SubscriptionException, FailureException {
        logger.info("Unsubscribing from item [{}]", itemName);
        subscriptionsHandler.unsubscribe(itemName);
    }
}
