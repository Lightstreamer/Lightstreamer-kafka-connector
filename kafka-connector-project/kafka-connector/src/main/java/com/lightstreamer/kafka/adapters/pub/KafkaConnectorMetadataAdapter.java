
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

package com.lightstreamer.kafka.adapters.pub;

import com.lightstreamer.adapters.metadata.LiteralBasedProvider;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.GlobalConfig;
import com.lightstreamer.kafka.common.config.ConfigException;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of a Lightstreamer Metadata Adapter for Lightstreamer Kafka Connector.
 *
 * <p>For the sake of simplicity, this documentation shows only the hook methods.
 */
public class KafkaConnectorMetadataAdapter extends LiteralBasedProvider {

    private static KafkaConnectorMetadataAdapter METADATA_ADAPTER;

    private final Map<String, ConnectionInfo> registeredDataAdapters = new ConcurrentHashMap<>();

    private final Map<String, Map<String, TableInfo>> tablesBySession = new ConcurrentHashMap<>();

    private Logger log;

    private GlobalConfig globalConfig;

    /**
     * @hidden
     */
    @SuppressWarnings("unchecked")
    @Override
    public final void init(@SuppressWarnings("rawtypes") Map params, File configDir)
            throws MetadataProviderException {
        super.init(params, configDir);
        globalConfig = GlobalConfig.newConfig(configDir, params);
        configureLogging(configDir);
        METADATA_ADAPTER = this;
        postInit(params, configDir);
    }

    /**
     * Hook method invoked by Lightstreamer Kernel after the initialization phase of the Kafka
     * Connector Metadata Adapter has been completed.
     *
     * @param params a Map-type value object that contains name-value pairs corresponding to the
     *     {@code param} elements supplied in the KafkaConnector configuration file under the {@code
     *     <metadata_provider>} element
     * @param configDir the path of the directory on the local disk where the KafkaConnector
     *     configuration file resides
     * @throws MetadataProviderException if an error occurs that prevents the correct behavior of
     *     the Metadata Adapter. This causes the Server not to complete the startup and to exit.
     * @see <a
     *     href="https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProvider.html#init(java.util.Map,java.io.File)">MetadataProvider.init(java.util.Map,
     *     java.io.File)</a>
     */
    protected void postInit(Map params, File configDir) throws MetadataProviderException {}

    /**
     * Only used for unit testing.
     *
     * @param sessionId a Session ID
     * @return an Optional describing the set of all the items associated with the specified session
     * @hidden
     */
    protected final Optional<Set<String>> itemsBySession(String sessionId) {
        return Optional.ofNullable(tablesBySession.get(sessionId)).map(m -> Set.copyOf(m.keySet()));
    }

    private void configureLogging(File configDir) throws ConfigException {
        String logConfigFile = globalConfig.getFile(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        PropertyConfigurator.configure(logConfigFile);
        this.log = LoggerFactory.getLogger(KafkaConnectorMetadataAdapter.class);
    }

    /**
     * Only used internally.
     *
     * <p>Returns a new MetadataLister instance, which will be used by the DataProvider to force
     * unsubscription from items.
     *
     * @param dataProviderName the name of the DataProvider
     * @param enabled indicates whether the calling DataProvider is enabled
     * @return a new MetadataListener instance
     * @hidden
     */
    public static final MetadataListener listener(String dataProviderName, boolean enabled) {
        return new MetadataListenerImpl(METADATA_ADAPTER, dataProviderName, enabled);
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

    /**
     * @hidden
     */
    @Override
    public final boolean wantsTablesNotification(String user) {
        return true;
    }

    /**
     * @hidden
     */
    @Override
    public void notifyNewTables(
            @Nullable String user, @Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws CreditsException, NotificationException {
        onSubscription(user, sessionID, tables);

        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];
        Optional<ConnectionInfo> connectionInfo = lookUp(table.getDataAdapter());
        if (connectionInfo.isPresent()) {
            registerConnectorItems(sessionID, table, connectionInfo.get());
        }
    }

    private void registerConnectorItems(
            String sessionID, TableInfo table, ConnectionInfo connection) throws CreditsException {
        if (!connection.enabled()) {
            throw new CreditsException(
                    -1, "Connection [%s] not enabled".formatted(connection.name()));
        }
        if (Mode.COMMAND.equals(table.getMode())) {
            throw new CreditsException(
                    -2, "Subscription mode [%s] not allowed".formatted(Mode.COMMAND));
        }

        String[] items = table.getSubscribedItems();
        for (String item : items) {
            Map<String, TableInfo> tables =
                    tablesBySession.computeIfAbsent(sessionID, id -> new ConcurrentHashMap<>());
            tables.put(item, table);
            log.atDebug().log("Added subscription [{}] to session [{}]", item, sessionID);
        }
    }

    /**
     * @hidden
     */
    @Override
    public final void notifyTablesClose(@Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws NotificationException {
        if (tables.length > 1) {
            throw new RuntimeException();
        }

        TableInfo table = tables[0];

        if (registeredDataAdapters.containsKey(table.getDataAdapter())) {
            String[] items = table.getSubscribedItems();
            Map<String, TableInfo> tablesByItem = tablesBySession.get(sessionID);
            for (String item : items) {
                tablesByItem.remove(item);
                log.atDebug().log("Removed subscription [{}] from session [{}]", item, sessionID);
            }
        }

        onUnsubscription(sessionID, tables);
    }

    private void notifyDataAdapter(String connectionName, boolean enabled) {
        registeredDataAdapters.put(connectionName, new ConnectionInfo(connectionName, enabled));
    }

    /**
     * Used only for unit testing.
     *
     * <p>Returns a {@link ConnectionInfo} instance that describes a configured connection.
     *
     * @param connectionName the name of the connection as specified in the attribute {@code name}
     *     of the {@code data_provider} block in the {@code adapters.xml} file
     * @return an Optional describing the connection info relative to the specified connection name
     * @hidden
     */
    protected final Optional<ConnectionInfo> lookUp(String connectionName) {
        return Optional.ofNullable(registeredDataAdapters.get(connectionName));
    }

    /**
     * Hook method invoked by Lightstreamer Kernel to notify that a user has submitted a
     * Subscription.
     *
     * <p>In this default implementation, the KafkaConnector Metadata Adapter does nothing.
     *
     * @param user a user name
     * @param sessionID the ID of a session owned by the user
     * @param tables an array of TableInfo instances, each of them containing the details of a
     *     Subscription to be added to the session
     * @throws CreditsException if the user is not allowed to submit the Subscription
     * @throws NotificationException if something is wrong in the parameters, such as the {@code
     *     sessionID} of a session that is not currently open or inconsistent information about the
     *     Subscription
     * @see <a
     *     href="https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProvider.html#notifyNewTables(java.lang.String,java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)">MetadataProvider.notifyNewTables(java.lang.String,
     *     java.lang.String, com.lightstreamer.interfaces.metadata.TableInfo)</a>
     */
    public void onSubscription(
            @Nullable String user, @Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws CreditsException, NotificationException {}

    /**
     * Hook method invoked by Lightstreamer Kernel to notify that a Subscription has been removed
     * from a push session.
     *
     * <p>In this default implementation, the KafkaConnector Metadata Adapter does nothing.
     *
     * @param sessionID the ID of a session owned by the user
     * @param tables an array of TableInfo instances, each of them containing the details of a
     *     Subscription that has been removed from the session
     * @throws NotificationException if something is wrong in the parameters, such as the {@code
     *     sessionID} of a session that is not currently open or inconsistent information about the
     *     Subscription
     * @see <a
     *     href="https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProvider.html#notifyTablesClose(java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)">MetadataProvider.notifyTablesClose(java.lang.String,
     *     com.lightstreamer.interfaces.metadata.TableInfo)</a>
     */
    public void onUnsubscription(@Nonnull String sessionID, @Nonnull TableInfo[] tables)
            throws NotificationException {}

    /**
     * Used only internally.
     *
     * @hidden
     */
    protected static record ConnectionInfo(String name, boolean enabled) {}

    private static class MetadataListenerImpl implements MetadataListener {

        private final String dataAdapter;

        private final KafkaConnectorMetadataAdapter metadataAdapter;

        private MetadataListenerImpl(
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
