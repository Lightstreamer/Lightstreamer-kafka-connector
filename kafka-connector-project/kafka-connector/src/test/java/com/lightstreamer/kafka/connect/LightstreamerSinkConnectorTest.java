
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

package com.lightstreamer.kafka.connect;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.ITEM_TEMPLATES;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_USERNAME;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_MAPPING;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.TOPIC_MAPPINGS;

import com.lightstreamer.kafka.test_utils.VersionUtils;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LightstreamerSinkConnectorTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(TOPIC_MAPPINGS, "topic:item1");
        config.put(RECORD_MAPPING, "field1:#{VALUE}");
        return config;
    }

    LightstreamerSinkConnector createConnector() {
        return new LightstreamerSinkConnector();
    }

    @Test
    void shouldGetVersion() {
        LightstreamerSinkConnector connector = createConnector();
        assertThat(connector.version()).isEqualTo(VersionUtils.currentVersion());
    }

    @Test
    void shouldGetTaskClass() {
        LightstreamerSinkConnector connector = createConnector();
        assertThat(connector.taskClass()).isEqualTo(LightstreamerSinkConnectorTask.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @ValueSource(ints = {1, 2, 3})
    void shouldShouldStartAndStop(int maxTasks) {
        LightstreamerSinkConnector connector = createConnector();
        Map<String, String> configs = basicConfig();
        connector.start(configs);

        assertThat(connector.configs()).isEqualTo(basicConfig());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        // The connector always returns a single configuration, regardless of the configured
        // maxTasks.
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs).containsExactly(basicConfig());

        connector.stop();
        assertThat(connector.configs()).isEmpty();
    }

    @Test
    void shouldGetConfig() {
        LightstreamerSinkConnector connector = createConnector();
        ConfigDef config = connector.config();
        Set<String> configKeys = config.configKeys().keySet();
        assertThat(configKeys)
                .containsExactly(
                        LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS,
                        LIGHTSTREAMER_PROXY_ADAPTER_USERNAME,
                        LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD,
                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS,
                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES,
                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS,
                        TOPIC_MAPPINGS,
                        ITEM_TEMPLATES,
                        RECORD_MAPPING,
                        RECORD_EXTRACTION_ERROR_STRATEGY);
    }
}
