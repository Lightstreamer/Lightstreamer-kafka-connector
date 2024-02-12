
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

import com.lightstreamer.interfaces.metadata.MetadataProviderException;
import com.lightstreamer.kafka_connector.adapters.config.ConfigException;
import com.lightstreamer.kafka_connector.adapters.config.GlobalConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class ConnectorMetadataAdapterTest {

    private Path adapterDir;
    private ConnectorMetadataAdapter connectorMetadataAdapter;
    private HashMap<String, String> params;
    private Path loggingConfigurationFile;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");

        connectorMetadataAdapter = new ConnectorMetadataAdapter();
        params = new HashMap<>();
        params.put("adapters_conf.id", "KAFKA");
    }

    @Test
    void shouldNotInitDueToMissingRequiredParameters() {
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> connectorMetadataAdapter.init(params, adapterDir.toFile()));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [logging.configuration.path]");
    }

    @Test
    void shouldInit() throws MetadataProviderException {
        params.put(
                GlobalConfig.LOGGING_CONFIGURATION_PATH,
                loggingConfigurationFile.getFileName().toString());
        connectorMetadataAdapter.init(params, adapterDir.toFile());
    }
}
