
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.common.config.ConfigException;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

class GlobalConfigTest {

    private Path loggingConfigurationFile;
    private Path adapterDir;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");
    }

    @AfterEach
    void after() throws IOException {
        Files.delete(loggingConfigurationFile);
        FileUtils.deleteDirectory(adapterDir.toFile());
    }

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = GlobalConfig.configSpec();

        ConfParameter adaptersConfId = configSpec.findParameter(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.name()).isEqualTo(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.required()).isTrue();
        assertThat(adaptersConfId.multiple()).isFalse();
        assertThat(adaptersConfId.mutable()).isTrue();
        assertThat(adaptersConfId.defaultValue()).isNull();
        assertThat(adaptersConfId.type()).isEqualTo(TEXT);

        ConfParameter loggingConfigurationPath =
                configSpec.findParameter(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationPath.name())
                .isEqualTo(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationPath.required()).isTrue();
        assertThat(loggingConfigurationPath.multiple()).isFalse();
        assertThat(loggingConfigurationPath.mutable()).isTrue();
        assertThat(loggingConfigurationPath.defaultValue()).isNull();
        assertThat(loggingConfigurationPath.type()).isEqualTo(FILE);
    }

    @Test
    void shouldSpecifyRequiredParam() {
        ConfigException e =
                assertThrows(ConfigException.class, () -> new GlobalConfig(Collections.emptyMap()));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]".formatted(GlobalConfig.ADAPTERS_CONF_ID));

        Map<String, String> params = new HashMap<>();
        params.put(GlobalConfig.ADAPTERS_CONF_ID, "");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.ADAPTERS_CONF_ID));

        params.put(GlobalConfig.ADAPTERS_CONF_ID, "KAFKA");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_PATH));

        params.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, "non-existing-conf-file");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
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

        // Ensure we are specifying a path name relative to the provided adapter dir.
        String farthestPathName = loggingConfigurationFile.getFileName().toString();
        adapterParams.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, farthestPathName);

        return adapterParams;
    }

    @Test
    void shouldGetNewConfig() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), minimal());
        assertThat(config.getText(GlobalConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getFile(GlobalConfig.LOGGING_CONFIGURATION_PATH))
                .isEqualTo(loggingConfigurationFile.toString());
    }

    static Stream<Named<Function<GlobalConfig, Object>>> publishSettingsAccessors() {
        return Stream.of(
                Named.of("getPublishKeyEvaluatorType", GlobalConfig::getPublishKeyEvaluatorType),
                Named.of(
                        "getPublishValueEvaluatorType", GlobalConfig::getPublishValueEvaluatorType),
                Named.of("baseProducerProps", GlobalConfig::baseProducerProps));
    }

    @ParameterizedTest
    @MethodSource("publishSettingsAccessors")
    void shouldNotAccessPublishSettings(Function<GlobalConfig, Object> accessor) {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), minimal());
        assertThat(config.isPublishEnabled()).isFalse();

        ConfigException ce = assertThrows(ConfigException.class, () -> accessor.apply(config));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Publish is not enabled. Check parameter [%s]"
                                .formatted(GlobalConfig.PUBLISH_ENABLE));
    }

    @Test
    void shouldSpecifyPublishParametersWhenRequired() {
        Map<String, String> params = minimal();
        params.put(GlobalConfig.PUBLISH_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.PUBLISH_BOOTSTRAP_SERVERS));

        params.put(GlobalConfig.PUBLISH_BOOTSTRAP_SERVERS, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PUBLISH_BOOTSTRAP_SERVERS));

        params.put(GlobalConfig.PUBLISH_BOOTSTRAP_SERVERS, "localhost:9092");
        assertDoesNotThrow(() -> GlobalConfig.newConfig(adapterDir.toFile(), params));
    }

    @Test
    void shouldRetrieveBaseProducerProperties() {
        Map<String, String> params = minimal();
        params.put(GlobalConfig.PUBLISH_ENABLE, "true");
        params.put(GlobalConfig.PUBLISH_BOOTSTRAP_SERVERS, "localhost:9092");

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsAtLeast(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000",
                        ProducerConfig.ACKS_CONFIG, "all");
    }
}
