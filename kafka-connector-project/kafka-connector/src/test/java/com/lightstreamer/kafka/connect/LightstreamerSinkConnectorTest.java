
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
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.FIELD_MAPPINGS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.ITEM_TEMPLATES;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRIES_COUNT;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_USERNAME;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.TOPIC_MAPPINGS;

import com.google.common.truth.Ordered;
import com.lightstreamer.kafka.test_utils.VersionUtils;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LightstreamerSinkConnectorTest {

    private Ordered containsExactly;

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:");
        config.put(TOPIC_MAPPINGS, "item1");
        config.put(FIELD_MAPPINGS, "field1:#{VALUE}");
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
    void shouldShouldStartAndStop() {
        LightstreamerSinkConnector connector = createConnector();
        Map<String, String> configs = basicConfig();
        connector.start(configs);

        assertThat(connector.configs()).isEqualTo(basicConfig());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).containsExactly(basicConfig());

        taskConfigs = connector.taskConfigs(2);
        assertThat(taskConfigs).containsExactly(configs, configs);

        connector.stop();
        assertThat(connector.configs()).isEmpty();
    }

    @Test
    void shouldGetConfig() {
        LightstreamerSinkConnector connector = createConnector();
        ConfigDef config = connector.config();
        Set<String> configKeys = config.configKeys().keySet();
        containsExactly =
                assertThat(configKeys)
                        .containsExactly(
                                LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS,
                                LIGHTSTREAMER_PROXY_ADAPTER_USERNAME,
                                LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD,
                                LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS,
                                LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRIES_COUNT,
                                LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS,
                                TOPIC_MAPPINGS,
                                ITEM_TEMPLATES,
                                FIELD_MAPPINGS,
                                RECORD_EXTRACTION_ERROR_STRATEGY);
    }

    @Test
    void shouldGetTaskConfigs() {
        LightstreamerSinkConnector connector = createConnector();
        connector.start(basicConfig());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).containsExactly(basicConfig());
    }
}
