
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

package com.lightstreamer.kafka_connector.adapters;

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SmartDataProvider;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapters.ConsumerLoopConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapters.commons.LogFactory;
import com.lightstreamer.kafka_connector.adapters.commons.MetadataListener;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.InfoItem;
import com.lightstreamer.kafka_connector.adapters.consumers.ConsumerLoop;
import java.io.File;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

public final class ConnectorDataAdapter implements SmartDataProvider {

    private Logger log;
    private Loop loop;
    private ConsumerLoopConfig<?, ?> loopConfig;
    private ConnectorConfig connectorConfig;
    private MetadataListener metadataAdapter;

    public ConnectorDataAdapter() {}

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Map params, @Nonnull File configDir) throws DataProviderException {
        this.connectorConfig = ConnectorConfig.newConfig(configDir, params);
        this.log = LogFactory.getLogger(connectorConfig.getAdapterName());
        this.metadataAdapter = ConnectorMetadataAdapter.listener(connectorConfig.getAdapterName());

        if (!connectorConfig.isEnabled()) {
            metadataAdapter.disableAdapter();
        }

        log.info("Configuring Kafka Connector");
        loopConfig = ConsumerLoopConfigurator.configure(connectorConfig);
        log.info("Configuration complete");
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return false;
    }

    private <K, V> Loop loop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        return new ConsumerLoop<>(config, metadataAdapter, eventListener);
    }

    private Loop makeLoop(ConsumerLoopConfig<?, ?> config, ItemEventListener eventListener) {
        return loop(config, eventListener);
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        this.loop = makeLoop(loopConfig, eventListener);
    }

    @Override
    public void subscribe(@Nonnull String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {}

    @Override
    public void subscribe(
            @Nonnull String itemName, @Nonnull Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        if (itemName.equals(connectorConfig.getText(ConnectorConfig.ITEM_INFO_NAME))) {
            log.atInfo().log("Subscribing to the special INFO item");
            loop.subscribeInfoItem(
                    new InfoItem(
                            itemHandle, connectorConfig.getText(ConnectorConfig.ITEM_INFO_FIELD)));
        } else {
            log.info("Trying subscription to item [{}]", itemName);
            loop.subscribe(itemName, itemHandle);
        }
    }

    @Override
    public void unsubscribe(@Nonnull String itemName)
            throws SubscriptionException, FailureException {
        if (itemName.equals(connectorConfig.getText(ConnectorConfig.ITEM_INFO_NAME))) {
            log.atInfo().log("Unsubscribing from the special the item [{}]", itemName);
            loop.unsubscribeInfoItem();
            return;
        }
        log.info("Unsubscribing from item [{}]", itemName);
        loop.unsubscribe(itemName);
    }
}
