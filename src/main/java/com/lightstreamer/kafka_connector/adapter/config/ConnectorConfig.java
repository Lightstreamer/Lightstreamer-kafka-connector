package com.lightstreamer.kafka_connector.adapter.config;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.Type;
import com.lightstreamer.kafka_connector.adapter.mapping.TopicMapping;

public class ConnectorConfig {

    public static final String ADAPTER_DIR = "adapter.dir";

    public static final String MAP = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String FIELD = "field";

    public static final String KEY_EVALUATOR_TYPE = "key-evaluator.type";

    public static final String KEY_SCHEMA_FILE = "key.evaluator.schema.file";

    public static final String VALUE_EVALUATOR_TYPE = "value.evaluator.type";

    public static final String VALUE_SCHEMA_FILE = "value.evaluator.schema.file";

    public static final String GROUP_ID = "group-id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap-servers";

    public static final String KEY_EVALUATOR_SCHEMA_REGISTRY_URL = "key.evaluator.schema.registry.url";

    public static final String VALUE_EVALUATOR_SCHEMA_REGISTRY_URL = "value.evaluator.schema.registry.url";

    public static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add(ADAPTER_DIR, true, false, ConfType.Directory)
                .add(BOOTSTRAP_SERVERS, true, false, ConfType.HostsList)
                .add(GROUP_ID, true, false, ConfType.Text)
                .add(MAP, true, true, MAP_SUFFIX, ConfType.Text)
                .add(FIELD, true, true, ConfType.Text)
                .add(KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, ConfType.Host)
                .add(VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, ConfType.Host)
                .add(KEY_EVALUATOR_TYPE, false, false, ConfType.Text, "RAW")
                .add(KEY_SCHEMA_FILE, false, false, ConfType.Text)
                .add(VALUE_EVALUATOR_TYPE, false, false, ConfType.Text, "RAW")
                .add(VALUE_SCHEMA_FILE, false, false, ConfType.Text);
    }

    private final ConfigSpec configSpec;

    private final Map<String, String> configuration;

    private ConnectorConfig(ConfigSpec spec, Map<String, String> configs) {
        this.configSpec = spec;
        this.configuration = Collections.unmodifiableMap(this.configSpec.parse(configs));
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

    public String getHost(String configKey, boolean forceRequired) {
        String value = get(configKey, ConfType.Host);
        if (forceRequired && value == null) {
            throw new ConfigException("Missing required parameter [%s]".formatted(configKey));
        }
        return value;
    }

    public String getHostsList(String configKey) {
        return get(configKey, ConfType.HostsList);
    }

    public String getDirectory(String configKey) {
        return get(configKey, ConfType.Directory);
    }

    public Map<String, String> getValues(String configKey) {
        ConfParameter param = this.configSpec.getParameter(configKey);
        if (param.multiple()) {
            Map<String, String> remap = new HashMap<>();
            for (Map.Entry<String, String> e : configuration.entrySet()) {
                Optional<String> infix = ConfigSpec.extractInfix(param, e.getKey());
                if (infix.isPresent()) {
                    remap.put(infix.get(), e.getValue());
                }
            }
            return remap;
        }
        return Collections.emptyMap();
    }

    public <T> List<T> getAsList(String configKey, Function<? super Map.Entry<String, String>, T> conv) {
        Map<String, String> values = getValues(configKey);
        return values.entrySet().stream().map(conv).toList();
    }

    public Properties baseConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public Map<String, ?> extendsConsumerProps(Map<String, String> props) {
        Map<String, String> extendedProps = new HashMap<>(baseConsumerProps()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));
        extendedProps.putAll(props);
        return extendedProps;
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

    // public static Map<String, String> append(Map<String, String>)

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put(ConnectorConfig.GROUP_ID, "group-id");
        map.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "consumer");
        map.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "consumer");
        map.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
        map.put(ConnectorConfig.ADAPTER_DIR, "test");
        map.put("map.topic1.a.to", "item-template1");
        map.put("map.topic2.b.to", "item-template2");
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
