package com.lightstreamer.kafka_connector.adapter.config;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ListType;
import com.lightstreamer.kafka_connector.adapter.mapping.TopicMapping;

public class ConnectorConfig {

    public static final String ADAPTER_DIR = "adapter.dir";

    public static final String MAP = "map.";

    public static final String FIELD = "field.";

    public static final String KEY_CONSUMER = "key.consumer";

    public static final String KEY_SCHEMA_FILE = "key.schema.file";

    public static final String VALUE_CONSUMER = "value.consumer";

    public static final String VALUE_SCHEMA_FILE = "value.schema.file";

    public static final String GROUP_ID = "group-id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap-servers";

    public static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add(BOOTSTRAP_SERVERS, true, false, new ListType<ConfType>(ConfType.Host))
                .add(GROUP_ID, true, false, ConfType.Text)
                .add(VALUE_SCHEMA_FILE, false, false, ConfType.Text)
                .add(VALUE_CONSUMER, true, false, ConfType.Text)
                .add(KEY_SCHEMA_FILE, false, false, ConfType.Text)
                .add(KEY_CONSUMER, false, false, ConfType.Text)
                .add(FIELD, true, true, ConfType.Text)
                .add(MAP, true, true, ConfType.Text)
                .add(ADAPTER_DIR, true, false, ConfType.Directory);
    }

    private final ConfigSpec configSpec;

    private final Map<String, String> configuration;

    private ConnectorConfig(ConfigSpec spec, Map<String, String> configs) {
        this.configSpec = spec;
        this.configuration = this.configSpec.parse(configs);
    }

    public ConnectorConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    public Map<String, String> configuration() {
        return this.configuration;
    }

    public Map<String, String> getValues(String configKey) {
        ConfParameter param = this.configSpec.getParameter(configKey);
        if (param.multiple()) {
            return configuration.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(configKey))
                    .collect(Collectors.toMap(e -> e.getKey().split("\\.")[1], Map.Entry::getValue));
            // .filter(e -> e.getKey().startsWith(configKey))
            // .map(e -> confKey.split("\\.")[1])
            // .toList();
        }
        return Collections.emptyMap();
    }

    public <T> List<T> getAsList(String configKey, Function<? super Map.Entry<String, String>, T> conv) {
        Map<String, String> values = getValues(configKey);
        return values.entrySet().stream().map(conv).toList();
    }

    public static Map<String, String> appendAdapterDir(Map<String, String> config, File configDir) {
        Map<String, String> updated = new HashMap<>(config);
        updated.put(ADAPTER_DIR, configDir.getAbsolutePath());
        return updated;
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put(ConnectorConfig.GROUP_ID, "group-id");
        map.put(ConnectorConfig.VALUE_CONSUMER, "consumer");
        map.put(ConnectorConfig.KEY_CONSUMER, "consumer");
        map.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
        map.put(ConnectorConfig.ADAPTER_DIR, "test");
        map.put("map.topic1.to", "item-template1");
        map.put("map.topic2.to", "item-template2");
        map.put("field.fieldName1", "bar");
        map.put("field.fieldName2", "bar");
        ConnectorConfig config = new ConnectorConfig(map);
        var conf = config.configuration();
        System.out.println(conf);

        System.out.println(config.getValues(ConnectorConfig.FIELD));
        System.out.println(config.getValues(ConnectorConfig.MAP));
        List<TopicMapping> topicMappings = config.getAsList(ConnectorConfig.MAP,
                e -> new TopicMapping(e.getKey(), Arrays.asList(new String[] { e.getValue() })));
        System.out.println(topicMappings);

    }

}
