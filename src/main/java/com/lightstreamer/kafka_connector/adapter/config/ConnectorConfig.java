package com.lightstreamer.kafka_connector.adapter.config;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ListType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.Type;
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

    public static final String KEY_SCHEMA_REGISTRY_URL = "key.schema.registry.url";

    public static final String VALUE_SCHEMA_REGISTRY_URL = "value.schema.registry.url";

    public static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add(ADAPTER_DIR, true, false, ConfType.Directory)
                .add(BOOTSTRAP_SERVERS, true, false, new ListType<ConfType>(ConfType.Host))
                .add(GROUP_ID, true, false, ConfType.Text)
                .add(MAP, true, true, ConfType.Text)
                .add(FIELD, true, true, ConfType.Text)
                .add(KEY_SCHEMA_REGISTRY_URL, false, false, ConfType.Host)
                .add(VALUE_SCHEMA_REGISTRY_URL, false, false, ConfType.Host)
                .add(KEY_CONSUMER, false, false, ConfType.Text, "RAW")
                .add(KEY_SCHEMA_FILE, false, false, ConfType.Text)
                .add(VALUE_CONSUMER, false, false, ConfType.Text, "RAW")
                .add(VALUE_SCHEMA_FILE, false, false, ConfType.Text);
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

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }

    private String get(String key, Type type) {
        ConfParameter param = configSpec.getParameter(key);
        if (param.type().equals(type)) {
            if (param.required() && configuration.containsKey(key)) {
                return configuration.get(key);
            } else {
                return configuration.getOrDefault(key, param.defaultValue());
            }
        }
        throw new ConfigException(
                "No parameter [%s] of %s type is present in the configuration".formatted(key, type));

    }

    public String getText(String configKey) {
        return get(configKey, ConfType.Text);
    }

    public String getHost(String configKey) {
        return get(configKey, ConfType.Host);
    }

    public String getDirectory(String configKey) {
        return get(configKey, ConfType.Directory);
    }

    public Map<String, String> getValues(String configKey) {
        ConfParameter param = this.configSpec.getParameter(configKey);
        if (param.multiple()) {
            return configuration.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(configKey))
                    .collect(Collectors.toMap(e -> e.getKey().split("\\.")[1], Map.Entry::getValue));
        }
        return Collections.emptyMap();
    }

    public <T> List<T> getAsList(String configKey, Function<? super Map.Entry<String, String>, T> conv) {
        Map<String, String> values = getValues(configKey);
        return values.entrySet().stream().map(conv).toList();
    }

    public Properties baseConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getText(BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ADAPTER_DIR, getDirectory(ADAPTER_DIR));

        String keySchemaFile = getText(KEY_SCHEMA_FILE);
        if (keySchemaFile != null) {
            properties.setProperty(KEY_SCHEMA_FILE, keySchemaFile);
        }

        String valueSchemaFile = getText(VALUE_SCHEMA_FILE);
        if (valueSchemaFile != null) {
            properties.setProperty(VALUE_SCHEMA_FILE, valueSchemaFile);
        }

        return properties;
    }

    public Map<String, ?> extendsonsumerProps(Map<String, String> props) {
        return baseConsumerProps()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    public boolean hasKeySchemaFile() {
        return getText(KEY_SCHEMA_FILE) != null;
    }

    public boolean hasValueSchemaFile() {
        return getText(VALUE_SCHEMA_FILE) != null;
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
