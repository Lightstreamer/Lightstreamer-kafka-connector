
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GlobalConfigTest {

    private Path loggingConfigurationFile;

    @BeforeEach
    public void before() throws IOException {
        loggingConfigurationFile = Files.createTempFile("log4j-", ".properties");
    }

    private Map<String, String> standardParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(
                GlobalConfig.LOGGING_CONFIGURATION_FILE, loggingConfigurationFile.toString());
        return adapterParams;
    }

    @Test
    public void shouldReturnConfigSpec() {
        ConfigSpec configSpec = GlobalConfig.configSpec();

        ConfParameter loggingConfigurationFile =
                configSpec.getParameter(GlobalConfig.LOGGING_CONFIGURATION_FILE);
        assertThat(loggingConfigurationFile.name())
                .isEqualTo(GlobalConfig.LOGGING_CONFIGURATION_FILE);
        assertThat(loggingConfigurationFile.required()).isTrue();
        assertThat(loggingConfigurationFile.multiple()).isFalse();
        assertThat(loggingConfigurationFile.mutable()).isTrue();
        assertThat(loggingConfigurationFile.defaultValue()).isNull();
        assertThat(loggingConfigurationFile.type()).isEqualTo(ConfType.FILE);
    }

    @Test
    public void shouldSpecifyRequiredParam() {
        ConfigException e =
                assertThrows(ConfigException.class, () -> new GlobalConfig(Collections.emptyMap()));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_FILE));

        Map<String, String> params = new HashMap<>();
        params.put(GlobalConfig.LOGGING_CONFIGURATION_FILE, "");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_FILE));

        params.put(GlobalConfig.LOGGING_CONFIGURATION_FILE, loggingConfigurationFile.toString());
        assertDoesNotThrow(() -> new GlobalConfig(params));
    }

    @Test
    public void shouldGetFile() {
        GlobalConfig config = new GlobalConfig(standardParameters());
        assertThat(config.getFile(GlobalConfig.LOGGING_CONFIGURATION_FILE))
                .isEqualTo(loggingConfigurationFile.toString());
    }
}
