
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
import com.lightstreamer.kafka.adapters.consumers.SnapshotStore;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.KafkaConnectorDataAdapterOpts;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

import org.slf4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
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
                                connectorConfig.consumeAtStartup(),
                                connectorConfig.implicitItemsEnabled(),
                                connectorConfig.isAutoCommandModeEnabled()
                                        || connectorConfig.isCommandEnforceEnabled()));

        this.logger.info("Configuring Kafka Connector");
        this.subscriptionsHandler = subscriptionHandler(configurator.consumerConfig());
        this.logger.info("Configuration complete");
    }

    private <K, V> SubscriptionsHandler<K, V> subscriptionHandler(Config<K, V> consumerConfig)
            throws DataProviderException {
        return SubscriptionsHandler.<K, V>builder()
                .withConsumerConfig(consumerConfig)
                .withMetadataListener(metadataListener)
                .atStartup(
                        connectorConfig.consumeAtStartup(), connectorConfig.implicitItemsEnabled())
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

    /**
     * Caches a raw Kafka record received from the Kafka consumer.
     *
     * <p>This method is called for each record consumed from the configured Kafka topics and serves
     * as the entry point for processing incoming Kafka data. The primary responsibility is to
     * populate an external cache that maintains the current state of Kafka records, which can later
     * be used to generate snapshot events for new subscriptions.
     *
     * <p>The external cache populated by this method is essential for providing snapshot data to
     * new subscribers via {@link #getSnapshotRecords}.
     *
     * @param kafkaRecord the raw Kafka record containing the key, value, headers, and metadata such
     *     as topic, partition, offset, and timestamp. This record represents the current state that
     *     should be cached and potentially forwarded as a realtime event.
     * @see #getSnapshotRecords(String, Map) for snapshot data retrieval
     */
    public void cacheKafkaRecord(RawKafkaRecord kafkaRecord) {}

    /**
     * Retrieves cached Kafka records to generate snapshot events for new subscriptions.
     *
     * <p>This method reads from the external cache that was previously populated by {@link
     * #cacheKafkaRecord} to provide snapshot data for new subscriptions. It serves as the primary
     * mechanism for delivering the current state of Kafka data to clients when they first subscribe
     * to an item.
     *
     * <p>The parameters represent the canonical form of a subscribed item, where the item prefix
     * and parameters together form the complete item name (e.g., "item-[a=value1,b=value2]"). This
     * canonical item name can match one or more configured item templates. The method should handle
     * cases where multiple templates match the same canonical form by applying the appropriate
     * cache query strategy for each matching template.
     *
     * <p><strong>Cache interaction:</strong>
     *
     * <ul>
     *   <li>Use the complete canonical form to identify matching item templates
     *   <li>Query the external cache according to each matching template's configuration
     *   <li>Aggregate and return records that represent the current state for the subscription
     *   <li>Ensure records are returned in a consistent order (e.g., by offset/timestamp)
     * </ul>
     *
     * <p>The returned records will be processed by the connector framework to generate snapshot
     * events that are sent to the subscribing client before realtime events begin flowing.
     *
     * @param itemPrefix the prefix part of the canonical item name (e.g., "orders" from
     *     "orders-[region=US,type=premium]").
     * @param parameters a map containing the parameter key-value pairs from the canonical item name
     *     (e.g., {region=US, type=premium}). May be empty if the canonical item name contains no
     *     parameters.
     * @return a collection of {@link RawKafkaRecord} objects representing the current cached state
     *     that matches the specified criteria. Returns an empty collection if no matching records
     *     are found in the cache. Returns null if the cache is unavailable or the operation cannot
     *     be completed.
     * @see #cacheKafkaRecord(RawKafkaRecord) for cache population
     */
    public Collection<RawKafkaRecord> getSnapshotRecords(
            String itemPrefix, Map<String, String> parameters) {
        return Collections.emptyList();
    }

    // Only for testing purposes
    public SubscriptionsHandler<?, ?> getSubscriptionsHandler() {
        return subscriptionsHandler;
    }

    private static class SnapshotStoreImpl implements SnapshotStore {

        private final KafkaConnectorDataAdapter adapter;

        SnapshotStoreImpl(KafkaConnectorDataAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public void put(RawKafkaRecord record) {
            adapter.cacheKafkaRecord(record);
        }

        @Override
        public Collection<RawKafkaRecord> get(SubscribedItem item) {
            return adapter.getSnapshotRecords(item.schema().name(), Collections.emptyMap());
        }
    }
}
