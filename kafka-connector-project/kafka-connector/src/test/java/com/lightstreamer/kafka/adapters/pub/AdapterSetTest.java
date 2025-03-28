
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.kafka.adapters.KafkaConnectorDataAdapter;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.GlobalConfig;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.ConnectionInfo;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AdapterSetTest {

    private Path adapterDir;
    private KafkaConnectorMetadataAdapter connectorMetadataAdapter;
    private HashMap<String, String> metadataAdapterParams;
    private Path loggingConfigurationFile;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");

        connectorMetadataAdapter = new KafkaConnectorMetadataAdapter();

        metadataAdapterParams = new HashMap<>();
        metadataAdapterParams.put("adapters_conf.id", "KAFKA");
    }

    void doInit() throws MetadataProviderException {
        doInit(connectorMetadataAdapter);
    }

    void doInit(KafkaConnectorMetadataAdapter adapter) throws MetadataProviderException {
        metadataAdapterParams.put(
                GlobalConfig.LOGGING_CONFIGURATION_PATH,
                loggingConfigurationFile.getFileName().toString());
        adapter.init(metadataAdapterParams, adapterDir.toFile());
    }

    @Test
    void shouldNotInitDueToMissingRequiredParameters() {
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () ->
                                connectorMetadataAdapter.init(
                                        metadataAdapterParams, adapterDir.toFile()));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [logging.configuration.path]");
    }

    @Test
    void shouldInit() throws MetadataProviderException {
        doInit();

        KafkaConnectorDataAdapter connectorDataAdapter1 = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfig();
        assertDoesNotThrow(
                () -> connectorDataAdapter1.init(dataAdapterParams, adapterDir.toFile()));

        KafkaConnectorDataAdapter connectorDataAdapter2 = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams2 =
                ConnectorConfigProvider.minimalConfigWith(
                        Map.of(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR2"));
        assertDoesNotThrow(
                () -> connectorDataAdapter2.init(dataAdapterParams2, adapterDir.toFile()));

        Optional<ConnectionInfo> connector1 = connectorMetadataAdapter.lookUp("CONNECTOR");
        assertThat(connector1).isPresent();
        assertThat(connector1.get().name()).isEqualTo("CONNECTOR");

        Optional<ConnectionInfo> connector2 = connectorMetadataAdapter.lookUp("CONNECTOR2");
        assertThat(connector2).isPresent();
        assertThat(connector2.get().name()).isEqualTo("CONNECTOR2");
    }

    @Test
    void shouldHandleConnectorItems() throws Exception {
        doInit();

        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfig();
        connectorDataAdapter.init(dataAdapterParams, adapterDir.toFile());

        TableInfo[] tables = mkTable("CONNECTOR", Mode.DISTINCT);
        connectorMetadataAdapter.notifyNewTables("user", "sessionId", tables);
        Optional<Set<String>> items = connectorMetadataAdapter.itemsBySession("sessionId");
        assertThat(items).isPresent();
        assertThat(items.get()).containsExactly("item1", "item2");
    }

    @Test
    void shouldNotHandleNonConnectorItems() throws Exception {
        doInit();

        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfig();
        connectorDataAdapter.init(dataAdapterParams, adapterDir.toFile());

        TableInfo[] tables = mkTable("OTHER-ADAPTER", Mode.DISTINCT);
        connectorMetadataAdapter.notifyNewTables("user", "sessionId", tables);
        Optional<Set<String>> items = connectorMetadataAdapter.itemsBySession("sessionId");
        assertThat(items.isPresent()).isFalse();
    }

    @Test
    void shouldDenyNotRegisteredConnection() throws Exception {
        doInit();

        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfig();
        dataAdapterParams.put(ConnectorConfig.ENABLE, "false");
        connectorDataAdapter.init(dataAdapterParams, adapterDir.toFile());

        TableInfo[] tables = mkTable("CONNECTOR", Mode.DISTINCT);
        CreditsException ce =
                assertThrows(
                        CreditsException.class,
                        () ->
                                connectorMetadataAdapter.notifyNewTables(
                                        "user", "sessionId", tables));
        assertThat(ce.getClientErrorCode()).isEqualTo(-1);
        assertThat(ce.getMessage()).isEqualTo("Connection [CONNECTOR] not enabled");
    }

    @Test
    void shouldDenyCommandSubscription() throws Exception {
        doInit();

        KafkaConnectorDataAdapter connectorDataAdapter = new KafkaConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfig();
        connectorDataAdapter.init(dataAdapterParams, adapterDir.toFile());

        TableInfo[] tables = mkTable("CONNECTOR", Mode.COMMAND);
        CreditsException ce =
                assertThrows(
                        CreditsException.class,
                        () ->
                                connectorMetadataAdapter.notifyNewTables(
                                        "user", "sessionId", tables));
        assertThat(ce.getClientErrorCode()).isEqualTo(-2);
        assertThat(ce.getMessage()).isEqualTo("Subscription mode [COMMAND] not allowed");
    }

    @Test
    void shouldHandleCustomAdapter() throws Exception {
        record NotifiedNewTables(String user, String sessionId, TableInfo[] tables) {}

        record NotifiedCloseTables(String sessionId, TableInfo[] tables) {}

        class CustomAdapter extends KafkaConnectorMetadataAdapter {

            NotifiedNewTables newTables;
            NotifiedCloseTables closedTables;

            @Override
            public void onSubscription(String user, String sessionID, TableInfo[] tables)
                    throws CreditsException, NotificationException {
                newTables = new NotifiedNewTables(user, sessionID, tables);
            }

            @Override
            public void onUnsubscription(String sessionID, TableInfo[] tables)
                    throws NotificationException {
                closedTables = new NotifiedCloseTables(sessionID, tables);
            }
        }
        CustomAdapter customAdapter = new CustomAdapter();
        doInit(customAdapter);

        TableInfo[] tables = mkTable("OTHER-ADAPTER", Mode.COMMAND);
        customAdapter.notifyNewTables("user", "sessionId", tables);

        NotifiedNewTables newTables = customAdapter.newTables;
        assertThat(newTables).isNotNull();
        assertThat(newTables.user()).isEqualTo("user");
        assertThat(newTables.sessionId()).isEqualTo("sessionId");
        assertThat(newTables.tables()).isEqualTo(tables);
        assertThat(customAdapter.closedTables).isNull();

        customAdapter.notifyTablesClose("sessionId", tables);
        NotifiedCloseTables closedTables = customAdapter.closedTables;
        assertThat(closedTables).isNotNull();
        assertThat(closedTables.sessionId()).isEqualTo("sessionId");
        assertThat(closedTables.tables()).isEqualTo(tables);
    }

    private TableInfo[] mkTable(String adapterName, Mode mode) {
        TableInfo[] tables = new TableInfo[1];
        TableInfo table =
                new TableInfo(
                        0,
                        mode,
                        "item",
                        adapterName,
                        "field",
                        1,
                        1,
                        "selector",
                        new String[] {"item1", "item2"},
                        null);
        tables[0] = table;
        return tables;
    }
}
