
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

import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.ItemsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderAdapter;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.SchemaException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.GlobalConfig;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;

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
public class KafkaConnectorMetadataAdapter extends MetadataProviderAdapter {

    private static KafkaConnectorMetadataAdapter METADATA_ADAPTER;

    private final Map<String, KafkaConnectorDataAdapterOpts> registeredDataAdapters =
            new ConcurrentHashMap<>();

    private final Map<String, Map<String, TableInfo>> tablesBySession = new ConcurrentHashMap<>();

    private Logger logger;

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
        logger.atInfo().log("Metadata Adapter \"{}\" initialized", params.get("adapters_conf.id"));
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
        this.logger = LoggerFactory.getLogger(KafkaConnectorMetadataAdapter.class);
    }

    /**
     * Only used internally.
     *
     * <p>Configuration options for a Kafka Connector Data Adapter.
     *
     * @param dataAdapterName the name of the Kafka Connector Data Adapter
     * @param enabled indicates whether the calling DataProvider is enabled
     * @param consumeAtStartup indicates whether the Kafka Connector should consume message at
     *     connector startup
     * @param implicitItemsEnabled indicates whether the Kafka Connector should allow implicit items
     * @param useCommandMode indicates whether the Kafka Connector is configured to use the command
     *     mode through one of the available settings ({@code fields.auto.command.mode.enable} and
     *     {@code fields.evaluate.as.command.enable} )
     */
    public static record KafkaConnectorDataAdapterOpts(
            String dataAdapterName,
            boolean enabled,
            boolean consumeAtStartup,
            boolean implicitItemsEnabled,
            boolean useCommandMode) {

        boolean supportMode(Mode mode) {
            if (implicitItemsEnabled() && useCommandMode()) {
                return Mode.COMMAND.equals(mode);
            }
            return true;
        }
    }

    /**
     * Only used internally.
     *
     * <p>Returns a new MetadataLister instance, which will be used by the DataProvider to force
     * unsubscription from items.
     *
     * @param opts the configuration options for the Kafka Connector Data Adapter
     * @return a new MetadataListener instance
     * @hidden
     */
    public static final MetadataListener listener(KafkaConnectorDataAdapterOpts opts) {
        return new MetadataListenerImpl(METADATA_ADAPTER, opts);
    }

    private void forceUnsubscriptionAll(String dataAdapterName) {
        logger.atWarn()
                .log(
                        "Forcing unsubscription from all active items of data adapter {}",
                        dataAdapterName);
        tablesBySession.values().stream()
                .flatMap(m -> m.values().stream())
                .filter(t -> t.getDataAdapter().equals(dataAdapterName))
                .forEach(this::forceUnsubscription);
    }

    private void forceUnsubscription(TableInfo tableInfo) {
        String items = Arrays.toString(tableInfo.getSubscribedItems());
        logger.atDebug().log("Forcing unsubscription from items {}", items);
        tableInfo
                .forceUnsubscription()
                .toCompletableFuture()
                .thenRun(() -> logger.atDebug().log("Forced unsubscription from item {}", items));
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
        Optional<KafkaConnectorDataAdapterOpts> opts = lookUp(table.getDataAdapter());
        if (opts.isPresent()) {
            registerConnectorItems(sessionID, table, opts.get());
        }
    }

    private void registerConnectorItems(
            String sessionID, TableInfo table, KafkaConnectorDataAdapterOpts opts)
            throws CreditsException {
        if (!opts.enabled()) {
            throw new CreditsException(
                    -1, "Connection [%s] not enabled".formatted(opts.dataAdapterName()));
        }

        String[] items = table.getSubscribedItems();
        for (String item : items) {
            Map<String, TableInfo> tables =
                    tablesBySession.computeIfAbsent(sessionID, id -> new ConcurrentHashMap<>());
            tables.put(item, table);
            logger.atDebug().log("Added subscription [{}] to session [{}]", item, sessionID);
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
                logger.atDebug().log(
                        "Removed subscription [{}] from session [{}]", item, sessionID);
            }
        }

        onUnsubscription(sessionID, tables);
    }

    /**
     * @hidden
     */
    @Override
    public final String[] getItems(
            String user, String sessionID, String itemList, String dataAdapter)
            throws ItemsException {
        String[] items = remapItems(user, sessionID, itemList, dataAdapter);
        String[] canonicalItems = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            canonicalItems[i] = Expressions.CanonicalItemName(items[i]);
        }
        return canonicalItems;
    }

    /**
     * Resolves a Field List specification supplied in a Request. The names of the Fields in the
     * List are returned.
     *
     * <p>Field List specifications are expected to be formed by simply concatenating the names of
     * the contained Fields, in a space separated way.
     *
     * @param user a User name
     * @param sessionID a Session ID
     * @param group the name of the Item Group (or specification of the Item List) whose Items the
     *     Schema is to be applied to
     * @param dataAdapter the name of the Data Adapter to which the subscription is targeted
     * @param schema a Field Schema name (or Field List specification)
     * @return an array with the names of the Fields in the Schema
     * @throws ItemsException if the supplied Item Group name (or Item List specification) is not
     *     recognized
     * @throws SchemaException if the supplied Field Schema name (or Field List specification) is
     *     not recognized
     */
    @Override
    public String[] getSchema(
            String user, String sessionID, String group, String dataAdapter, String schema)
            throws ItemsException, SchemaException {
        return schema.trim().split("\\s+");
    }

    /**
     * Remaps a list of items for a specific user session and data adapter.
     *
     * <p>This method processes a whitespace-separated string of items and converts it into an array
     * of individual item names. If the input is null or empty after trimming, an empty array is
     * returned.
     *
     * <p><strong>Override this method</strong> to provide custom item resolution logic
     *
     * @param user a User name
     * @param sessionID a Session ID
     * @param itemList an Item List specification (whitespace-separated item names)
     * @param dataAdapter the name of the Data Adapter to which the Item List is targeted
     * @return an array of strings containing the individual item names, or an empty array if the
     *     input is null or empty
     * @throws ItemsException if there are issues with the items during the remapping process
     */
    public String[] remapItems(String user, String sessionID, String itemList, String dataAdapter)
            throws ItemsException {
        if (itemList == null || itemList.trim().isEmpty()) {
            return new String[0];
        }

        return itemList.trim().split("\\s+");
    }

    private void notifyDataAdapter(KafkaConnectorDataAdapterOpts opts) {
        registeredDataAdapters.put(opts.dataAdapterName(), opts);
    }

    /**
     * Used only internally.
     *
     * <p>Returns a {@link KafkaConnectorDataAdapterOpts} instance that describes a configured
     * connection.
     *
     * @param connectionName the name of the connection as specified in the attribute {@code name}
     *     of the {@code data_provider} block in the {@code adapters.xml} file
     * @return an Optional describing the connection info relative to the specified connection name
     * @hidden
     */
    protected final Optional<KafkaConnectorDataAdapterOpts> lookUp(String connectionName) {
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

    private static class MetadataListenerImpl implements MetadataListener {

        private final KafkaConnectorMetadataAdapter metadataAdapter;
        private final KafkaConnectorDataAdapterOpts opts;

        private MetadataListenerImpl(
                KafkaConnectorMetadataAdapter metadataAdapter, KafkaConnectorDataAdapterOpts opts) {
            this.metadataAdapter = metadataAdapter;
            this.opts = opts;
            if (metadataAdapter != null) {
                metadataAdapter.notifyDataAdapter(opts);
            }
        }

        @Override
        public void forceUnsubscription(String item) {}

        @Override
        public void forceUnsubscriptionAll() {
            if (!opts.consumeAtStartup() && metadataAdapter != null) {
                metadataAdapter.forceUnsubscriptionAll(opts.dataAdapterName);
            }
        }
    }

    @Override
    public boolean modeMayBeAllowed(String item, String dataAdapter, Mode mode) {
        logger.atDebug().log(
                "Checking if mode {} is allowed for item {} in data adapter {}",
                mode,
                item,
                dataAdapter);

        Optional<KafkaConnectorDataAdapterOpts> opts = lookUp(dataAdapter);
        if (opts.isPresent()) {
            return opts.get().supportMode(mode);
        }

        return super.modeMayBeAllowed(item, dataAdapter, mode);
    }
}
