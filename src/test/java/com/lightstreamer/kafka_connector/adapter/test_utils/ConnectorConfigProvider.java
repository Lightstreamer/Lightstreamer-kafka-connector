package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public class ConnectorConfigProvider {

    public static Map<String, String> essentialConfigs() {
        try {
            return essentialConfigs(Files.createTempDirectory("adapter_dir"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> essentialConfigs(Path adapterDir) {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.GROUP_ID, "group-id");
        adapterParams.put("map.topic1.to", "item-template1");
        adapterParams.put("field.fieldName1", "bar");
        return adapterParams;
    }

    public static Map<String, String> essentialConfigsWith(Map<String, String> additionalConfigs) {
        Map<String, String> essentialConfigs = essentialConfigs();
        essentialConfigs.putAll(additionalConfigs);
        return essentialConfigs;
    }

    public static ConnectorConfig get() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.GROUP_ID, "group-id");
        configs.put(ConnectorConfig.VALUE_CONSUMER, "consumer");
        configs.put(ConnectorConfig.KEY_CONSUMER, "consumer");
        configs.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
        configs.put(ConnectorConfig.ADAPTER_DIR, "test");
        configs.put("map.topic1.to", "item-template1");
        configs.put("map.topic2.to", "item-template2");
        configs.put("field.fieldName1", "bar");
        configs.put("field.fieldName2", "bar");
        ConnectorConfig config = new ConnectorConfig(configs);
        return config;

    }

    public static void main(String[] args) {
        get();
    }

}
