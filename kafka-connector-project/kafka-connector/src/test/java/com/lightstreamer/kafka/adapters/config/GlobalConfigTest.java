
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

package com.lightstreamer.kafka.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.DIRECTORY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.common.config.ConfigException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GlobalConfigTest {

    private Path loggingConfigurationFile;
    private Path adapterDir;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");
    }

    @Test
    public void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = GlobalConfig.configSpec();

        ConfParameter adaptersConfId = configSpec.getParameter(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.name()).isEqualTo(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.required()).isTrue();
        assertThat(adaptersConfId.multiple()).isFalse();
        assertThat(adaptersConfId.mutable()).isTrue();
        assertThat(adaptersConfId.defaultValue()).isNull();
        assertThat(adaptersConfId.type()).isEqualTo(TEXT);

        ConfParameter adapterDir = configSpec.getParameter(GlobalConfig.ADAPTER_DIR);
        assertThat(adapterDir.name()).isEqualTo(GlobalConfig.ADAPTER_DIR);
        assertThat(adapterDir.required()).isTrue();
        assertThat(adapterDir.multiple()).isFalse();
        assertThat(adapterDir.mutable()).isTrue();
        assertThat(adapterDir.defaultValue()).isNull();
        assertThat(adapterDir.type()).isEqualTo(DIRECTORY);

        ConfParameter loggingConfigurationFile =
                configSpec.getParameter(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationFile.name())
                .isEqualTo(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationFile.required()).isTrue();
        assertThat(loggingConfigurationFile.multiple()).isFalse();
        assertThat(loggingConfigurationFile.mutable()).isTrue();
        assertThat(loggingConfigurationFile.defaultValue()).isNull();
        assertThat(loggingConfigurationFile.type()).isEqualTo(FILE);
    }

    @Test
    public void shouldSpecifyRequiredParam() {
        ConfigException e =
                assertThrows(ConfigException.class, () -> new GlobalConfig(Collections.emptyMap()));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]".formatted(GlobalConfig.ADAPTERS_CONF_ID));

        Map<String, String> params = new HashMap<>();
        params.put(GlobalConfig.ADAPTERS_CONF_ID, "");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.ADAPTERS_CONF_ID));

        params.put(GlobalConfig.ADAPTERS_CONF_ID, "KAFKA");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [%s]".formatted(GlobalConfig.ADAPTER_DIR));

        params.put(GlobalConfig.ADAPTER_DIR, "");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.ADAPTER_DIR));

        params.put(GlobalConfig.ADAPTER_DIR, adapterDir.toString());
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_PATH));

        params.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, "non-existing-conf-file");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Not found file [non-existing-conf-file] specified in [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_PATH));

        params.put(
                GlobalConfig.LOGGING_CONFIGURATION_PATH,
                Path.of(adapterDir.toString(), loggingConfigurationFile.getFileName().toString())
                        .toString());
        assertDoesNotThrow(() -> new GlobalConfig(params));
    }

    private Map<String, String> minimal() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());

        // Ensure we are specifying a path name relative to the provided adapter dir.
        String farthestPathName = loggingConfigurationFile.getFileName().toString();
        adapterParams.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, farthestPathName);

        return adapterParams;
    }

    @Test
    public void shouldGetNewConfig() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), minimal());
        assertThat(config.getText(GlobalConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getDirectory(GlobalConfig.ADAPTER_DIR)).isEqualTo(adapterDir.toString());
        assertThat(config.getFile(GlobalConfig.LOGGING_CONFIGURATION_PATH))
                .isEqualTo(loggingConfigurationFile.toString());
    }
}
