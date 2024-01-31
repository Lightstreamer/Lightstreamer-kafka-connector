/*
*
* Copyright (c) Lightstreamer Srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.adapters.metadata.LiteralBasedProvider;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka_connector.adapter.commons.MetadataListener;

public final class KafkaMetadataAdapter extends LiteralBasedProvider {

    private static Logger log = LoggerFactory.getLogger(KafkaMetadataAdapter.class);
    private static KafkaMetadataAdapter METADATA_ADAPTER;

    private Set<String> disabledDataProviders = ConcurrentHashMap.newKeySet();
    private Map<String, Map<String, TableInfo>> tablesBySession = new ConcurrentHashMap<>();

    @Override
    public void init(Map params, File configDir) throws MetadataProviderException {
        super.init(params, configDir);
        METADATA_ADAPTER = this;
    }

    public static MetadataListener listener(String dataProviderName) {
        return new MetadataListenterImpl(METADATA_ADAPTER, dataProviderName);
    }

    void disableDataProvider(String dataAdapter) {
        disabledDataProviders.add(dataAdapter);
    }

    private void forceUnsubscriptionAll(String dataAdapterName) {
        log.atDebug().log("Forcing unsubscription from all active items of data adapter {}", dataAdapterName);
        tablesBySession.values()
                .stream().flatMap(m -> m.values().stream())
                .filter(t -> t.getDataAdapter().equals(dataAdapterName))
                .forEach(this::forceUnsubscription);
    }

    private void forceUnsubscription(TableInfo tableInfo) {
        String items = Arrays.toString(tableInfo.getSubscribedItems());
        log.atDebug().log("Forcing unsubscription from items {}", items);
        tableInfo.forceUnsubscription()
                .toCompletableFuture()
                .thenRun(() -> log.atDebug().log("Forced unsubscription from item {}", items));
    }

    @Override
    public boolean wantsTablesNotification(String user) {
        return true;
    }

    @Override
    public void notifyNewTables(@Nullable String user, @Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws CreditsException, NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        if (disabledDataProviders.contains(table.getDataAdapter())) {
            throw new CreditsException(-1,
                    "DataProvider %s is out of work at the moment".formatted(table.getDataAdapter()));
        }

        String[] items = table.getSubscribedItems();
        for (String item : items) {
            Map<String, TableInfo> tablesByItem = tablesBySession.computeIfAbsent(sessionID,
                    id -> new ConcurrentHashMap<>());
            tablesByItem.put(item, table);
            log.atDebug().log("Added subscription <{}> to session <{}>", item, sessionID);
        }
    }

    @Override
    public void notifyTablesClose(@Nonnull String sessionID, @Nonnull TableInfo[] tables) throws NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        String[] items = table.getSubscribedItems();
        Map<String, TableInfo> tablesByItem = tablesBySession.get(sessionID);
        for (String item : items) {
            tablesByItem.remove(item);
            log.atDebug().log("Removed subscription <{}> from session {}", item, sessionID);
        }

    }

    static class MetadataListenterImpl implements MetadataListener {

        private final String dataAdapter;
        private KafkaMetadataAdapter metadataAdapter;

        private MetadataListenterImpl(KafkaMetadataAdapter metadataAdapter, String dataAdapter) {
            this.metadataAdapter = metadataAdapter;
            this.dataAdapter = dataAdapter;
        }

        @Override
        public void disableAdapter() {
            metadataAdapter.disableDataProvider(dataAdapter);
        }

        @Override
        public void forceUnsubscription(String item) {
        }

        @Override
        public void forceUnsubscriptionAll() {
            metadataAdapter.forceUnsubscriptionAll(dataAdapter);
        }

    }
}
