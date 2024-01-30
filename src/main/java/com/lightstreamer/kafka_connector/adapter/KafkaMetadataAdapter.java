package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.lightstreamer.adapters.metadata.LiteralBasedProvider;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka_connector.adapter.commons.MetadataListener;

public class KafkaMetadataAdapter extends LiteralBasedProvider {

    private static KafkaMetadataAdapter METADATA_ADAPTER;

    private Set<String> disabledDataProviders = ConcurrentHashMap.newKeySet();

    private Map<String, Map<String, TableInfo>> tableInfos = new ConcurrentHashMap<>();

    @Override
    public void init(Map params, File configDir) throws MetadataProviderException {
        super.init(params, configDir);
        METADATA_ADAPTER = this;
    }

    public static MetadataListener listener(String dataProviderName) {
        return new MetadataListenterImpl(METADATA_ADAPTER, dataProviderName);
    }

    void disableDataProvider(String dataProviderName) {
        disabledDataProviders.add(dataProviderName);
    }

    void forceUnsubscription(String item) {
        Map<String, TableInfo> tables = tableInfos.get(item);
        for (TableInfo tableInfo : tables.values()) {
            System.out.format("Forcing unsubscription from item [%s]%n", tableInfo);
            tableInfo.forceUnsubscription()
                    .toCompletableFuture()
                    .thenRun(() -> System.out.format("Forced unsubscription from item [%s]%n", item));
        }

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
            Map<String, TableInfo> infos = tableInfos.computeIfAbsent(item, id -> new ConcurrentHashMap<>());
            infos.put(sessionID, table);
            System.out.format("Notifyed table [%s]%n", item);
        }
    }

    @Override
    public void notifyTablesClose(@Nonnull String sessionID, @Nonnull TableInfo[] tables) throws NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        String[] items = table.getSubscribedItems();
        for (String item : items) {
            Map<String, TableInfo> infos = tableInfos.get(item);
            if (infos != null) {
                infos.remove(sessionID);
                System.out.format("Removed table [%s] from session [%s]%n", item, sessionID);
            }
        }

    }

    static class MetadataListenterImpl implements MetadataListener {

        private final String dataProviderName;
        private KafkaMetadataAdapter metadataAdapter;

        private MetadataListenterImpl(KafkaMetadataAdapter metadataAdapter, String dataProviderName) {
            this.metadataAdapter = metadataAdapter;
            this.dataProviderName = dataProviderName;
        }

        @Override
        public void disableAdapter() {
            metadataAdapter.disableDataProvider(dataProviderName);
        }

        @Override
        public void forceUnsubscription(String item) {
            metadataAdapter.forceUnsubscription(item);
        }

    }
}
