
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

package com.lightstreamer.kafka_connector.adapters.test_utils;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConnectorConfigProvider {

    private static Path createTempAdapterDir() {
        try {
            return Files.createTempDirectory("adapter_dir");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> minimalConfigParams() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ITEM_TEMPLATE, "item.template1");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put("map.topic1.to", "item-template1");
        adapterParams.put("field.fieldName1", "bar");
        return adapterParams;
    }

    private static Map<String, String> minimalConfigParamsWith(
            Map<String, String> additionalConfigs) {
        Map<String, String> essentialConfigs = minimalConfigParams();
        essentialConfigs.putAll(additionalConfigs);
        return essentialConfigs;
    }

    public static ConnectorConfig minimal() {
        return minimalWith(Collections.emptyMap());
    }

    public static ConnectorConfig minimal(Path adapterDir) {
        return minimalWith(Collections.emptyMap(), adapterDir);
    }

    public static ConnectorConfig minimalWith(Map<String, String> additionalConfigs) {
        return minimalWith(additionalConfigs, createTempAdapterDir());
    }

    public static ConnectorConfig minimalWith(
            Map<String, String> additionalConfigs, Path adapterDir) {
        return ConnectorConfig.newConfig(
                adapterDir.toFile(), minimalConfigParamsWith(additionalConfigs));
    }
}
