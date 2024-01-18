package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public class ConnectorConfigProvider {

    public static Map<String, String> minimalConfigParams() {
        try {
            return minimalConfigParams(Files.createTempDirectory("adapter_dir"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> minimalConfigParams(Path adapterDir) {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.GROUP_ID, "group-id");
        adapterParams.put(ConnectorConfig.ITEM_TEMPLATE, "item.template1");
        adapterParams.put("map.topic1.to", "item-template1");
        adapterParams.put("field.fieldName1", "bar");
        return adapterParams;
    }

    private static Map<String, String> minimalConfigParamsWith(Map<String, String> additionalConfigs, Path adapterDir) {
        Map<String, String> essentialConfigs = minimalConfigParams(adapterDir);
        essentialConfigs.putAll(additionalConfigs);
        return essentialConfigs;
    }

    private static Map<String, String> minimalConfigParamsWith(Map<String, String> additionalConfigs) {
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
        return new ConnectorConfig(minimalConfigParamsWith(additionalConfigs));
    }

    public static ConnectorConfig minimalWith(Map<String, String> additionalConfigs, Path adapterDir) {
        return new ConnectorConfig(minimalConfigParamsWith(additionalConfigs, adapterDir));
    }

}
