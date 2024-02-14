
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.kafka_connector.adapters.config.ConfigException;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.GlobalConfig;
import com.lightstreamer.kafka_connector.adapters.pub.KafkaConnectorMetadataAdapter.ConnectionInfo;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ConnectorAdapterSetTest {

    private Path adapterDir;
    private ConnectorMetadataAdapter connectorMetadataAdapter;
    private HashMap<String, String> metadataAdapterParams;
    private Path loggingConfigurationFile;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");

        connectorMetadataAdapter = new ConnectorMetadataAdapter();

        metadataAdapterParams = new HashMap<>();
        metadataAdapterParams.put("adapters_conf.id", "KAFKA");
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
        metadataAdapterParams.put(
                GlobalConfig.LOGGING_CONFIGURATION_PATH,
                loggingConfigurationFile.getFileName().toString());
        connectorMetadataAdapter.init(metadataAdapterParams, adapterDir.toFile());

        ConnectorDataAdapter connectorDataAdapter1 = new ConnectorDataAdapter();
        Map<String, String> dataAdapterParams = ConnectorConfigProvider.minimalConfigParams();
        assertDoesNotThrow(
                () -> connectorDataAdapter1.init(dataAdapterParams, adapterDir.toFile()));

        ConnectorDataAdapter connectorDataAdapter2 = new ConnectorDataAdapter();
        Map<String, String> dataAdapterParams2 =
                ConnectorConfigProvider.minimalConfigParamsWith(
                        Map.of(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR2"));
        assertDoesNotThrow(
                () -> connectorDataAdapter2.init(dataAdapterParams2, adapterDir.toFile()));

        ConnectionInfo connector1 = connectorMetadataAdapter.lookUp("CONNECTOR");
        assertThat(connector1.name()).isEqualTo("CONNECTOR");

        ConnectionInfo connector2 = connectorMetadataAdapter.lookUp("CONNECTOR2");
        assertThat(connector2.name()).isEqualTo("CONNECTOR2");
    }
}
