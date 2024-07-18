
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

import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.test_utils.VersionUtils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LightstreamerSinkConnectorTaskTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LightstreamerConnectorConfig.LIGHTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1");
        config.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:#{VALUE}");
        return config;
    }

    LightstreamerSinkConnectorTask createTask() {
        return new LightstreamerSinkConnectorTask();
    }

    @Test
    void shouldGetVersion() {
        LightstreamerSinkConnectorTask connector = createTask();
        assertThat(connector.version()).isEqualTo(VersionUtils.currentVersion());
    }

    @Test
    void shouldShouldStartAndStop() {
        LightstreamerSinkConnectorTask task = createTask();
        Map<String, String> configs = basicConfig();
        task.start(configs);

        // task.
        // assertThat(task.configs()).isEqualTo(basicConfig());
        // List<Map<String, String>> taskConfigs = task.taskConfigs(1);
        // assertThat(taskConfigs).containsExactly(basicConfig());

        // taskConfigs = task.taskConfigs(2);
        // assertThat(taskConfigs).containsExactly(configs, configs);

        // task.stop();
        // assertThat(task.configs()).isEmpty();
    }

    // @Test
    // void shouldGetConfig() {
    //     LightstreamerSinkConnector connector = createTask();
    //     ConfigDef config = connector.config();
    //     Set<String> configKeys = config.configKeys().keySet();
    //     assertThat(configKeys)
    //             .containsExactly(
    //                     LightstreamerConnectorConfig.LIGHTREAMER_PROXY_ADAPTER_ADDRESS,
    //                     LightstreamerConnectorConfig.TOPIC_MAPPINGS,
    //                     LightstreamerConnectorConfig.ITEM_TEMPLATES,
    //                     LightstreamerConnectorConfig.FIELD_MAPPINGS,
    //                     LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY);
    // }

    // @Test
    // void shouldGetTaskConfigs() {
    //     LightstreamerSinkConnector connector = createTask();
    //     connector.start(basicConfig());
    //     List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    //     assertThat(taskConfigs).containsExactly(basicConfig());
    // }
}
