
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

package com.lightstreamer.kafka_connector.adapters.pub;

import com.lightstreamer.adapters.metadata.LiteralBasedProvider;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka_connector.adapters.ConnectorMetadataAdapter;
import com.lightstreamer.kafka_connector.adapters.commons.MetadataListener;
import com.lightstreamer.kafka_connector.adapters.config.ConfigException;
import com.lightstreamer.kafka_connector.adapters.config.GlobalConfig;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class KafkaConnectorMetadataAdapter extends LiteralBasedProvider {

    private static KafkaConnectorMetadataAdapter METADATA_ADAPTER;

    private final Map<String, DataAdapterInfo> registeredDataADapters = new ConcurrentHashMap<>();

    private final Map<String, Map<String, TableInfo>> tablesBySession = new ConcurrentHashMap<>();

    private Logger log;

    private GlobalConfig globalConfig;

    @SuppressWarnings("unchecked")
    @Override
    public final void init(@SuppressWarnings("rawtypes") Map params, File configDir)
            throws MetadataProviderException {
        super.init(params, configDir);
        globalConfig = GlobalConfig.newConfig(configDir, params);
        configureLogging(configDir);
        METADATA_ADAPTER = this;
    }

    private void configureLogging(File configDir) throws ConfigException {
        String logConfigFile = globalConfig.getFile(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        PropertyConfigurator.configure(logConfigFile);
        this.log = LoggerFactory.getLogger(ConnectorMetadataAdapter.class);
    }

    public static final MetadataListener listener(String dataProviderName, boolean enabled) {
        return new MetadataListenterImpl(METADATA_ADAPTER, dataProviderName, enabled);
    }

    private void forceUnsubscriptionAll(String dataAdapterName) {
        log.atDebug().log(
                "Forcing unsubscription from all active items of data adapter {}", dataAdapterName);
        tablesBySession.values().stream()
                .flatMap(m -> m.values().stream())
                .filter(t -> t.getDataAdapter().equals(dataAdapterName))
                .forEach(this::forceUnsubscription);
    }

    private void forceUnsubscription(TableInfo tableInfo) {
        String items = Arrays.toString(tableInfo.getSubscribedItems());
        log.atDebug().log("Forcing unsubscription from items {}", items);
        tableInfo
                .forceUnsubscription()
                .toCompletableFuture()
                .thenRun(() -> log.atDebug().log("Forced unsubscription from item {}", items));
    }

    @Override
    public final boolean wantsTablesNotification(String user) {
        return true;
    }

    @Override
    public void notifyNewTables(
            @Nullable String user, @Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws CreditsException, NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        DataAdapterInfo dataAdapterInfo = registeredDataADapters.get(table.getDataAdapter());

        if (dataAdapterInfo != null) {
            if (!dataAdapterInfo.enabled()) {
                throw new CreditsException(
                        -1,
                        "DataProvider %s is out of work at the moment"
                                .formatted(dataAdapterInfo.name()));
            }

            String[] items = table.getSubscribedItems();
            for (String item : items) {
                Map<String, TableInfo> tablesByItem =
                        tablesBySession.computeIfAbsent(sessionID, id -> new ConcurrentHashMap<>());
                tablesByItem.put(item, table);
                log.atDebug().log("Added subscription <{}> to session <{}>", item, sessionID);
            }
        }

        newTables(user, sessionID, tables);
    }

    @Override
    public final void notifyTablesClose(@Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        if (registeredDataADapters.containsKey(table.getDataAdapter())) {
            String[] items = table.getSubscribedItems();
            Map<String, TableInfo> tablesByItem = tablesBySession.get(sessionID);
            for (String item : items) {
                tablesByItem.remove(item);
                log.atDebug().log("Removed subscription <{}> from session {}", item, sessionID);
            }
        }

        tableClosed(sessionID, tables);
    }

    private void notifyDataAdapter(String dataAdapterName, boolean enabled) {
        registeredDataADapters.put(dataAdapterName, new DataAdapterInfo(dataAdapterName, enabled));
    }

    public abstract void newTables(
            @Nullable String user, @Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws CreditsException, NotificationException;

    public abstract void tableClosed(@Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws NotificationException;

    @Override
    public final boolean isModeAllowed(
            @Nullable String user,
            @Nonnull String item,
            @Nonnull String dataAdapter,
            @Nonnull Mode mode) {
        return !Mode.COMMAND.equals(mode);
    }

    private static record DataAdapterInfo(String name, boolean enabled) {}

    private static class MetadataListenterImpl implements MetadataListener {

        private final String dataAdapter;

        private final KafkaConnectorMetadataAdapter metadataAdapter;

        private MetadataListenterImpl(
                KafkaConnectorMetadataAdapter metadataAdapter,
                String dataAdapter,
                boolean enabled) {
            this.metadataAdapter = metadataAdapter;
            this.dataAdapter = dataAdapter;
            this.metadataAdapter.notifyDataAdapter(dataAdapter, enabled);
        }

        @Override
        public void forceUnsubscription(String item) {}

        @Override
        public void forceUnsubscriptionAll() {
            metadataAdapter.forceUnsubscriptionAll(dataAdapter);
        }
    }
}
