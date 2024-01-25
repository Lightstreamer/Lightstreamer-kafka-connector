package com.lightstreamer.kafka_connector.adapter.config;

import static com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType.URL;
import static com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.DefaultHolder.defaultValue;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

import java.io.File;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.Type;

public class ConnectorConfig {

    public static final String DATA_ADAPTER_NAME = "data_provider.name";

    public static final String ADAPTERS_CONF_ID = "adapters_conf.id";

    public static final String ADAPTER_DIR = "adapter.dir";

    public static final String ITEM_TEMPLATE = "item-template";

    public static final String TOPIC_MAPPING = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String FIELD_MAPPING = "field";

    public static final String KEY_EVALUATOR_TYPE = "key.evaluator.type";

    public static final String KEY_SCHEMA_FILE = "key.evaluator.schema.file";

    public static final String VALUE_EVALUATOR_TYPE = "value.evaluator.type";

    public static final String VALUE_SCHEMA_FILE = "value.evaluator.schema.file";

    public static final String GROUP_ID = "group-id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap-servers";

    public static final String KEY_EVALUATOR_SCHEMA_REGISTRY_URL = "key.evaluator.schema.registry.url";

    public static final String VALUE_EVALUATOR_SCHEMA_REGISTRY_URL = "value.evaluator.schema.registry.url";

    public static final String ITEM_INFO_NAME = "info.item";

    public static final String ITEM_INFO_FIELD = "info.field";

    // Kafka consumer specific settings
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG = "consumer." + AUTO_OFFSET_RESET_CONFIG;

    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = "consumer." + ENABLE_AUTO_COMMIT_CONFIG;

    private static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add(ADAPTER_DIR, true, false, ConfType.Directory)
                .add(BOOTSTRAP_SERVERS, true, false, ConfType.HostsList)
                .add(GROUP_ID, false, false, TEXT, defaultValue(params -> {
                    String suffix = new SecureRandom()
                            .ints(20, 48, 122)
                            .mapToObj(Character::toString)
                            .collect(Collectors.joining());
                    return "%s-%s-%s".formatted(params.get(ADAPTERS_CONF_ID), params.get(DATA_ADAPTER_NAME), suffix);
                }))
                .add(ADAPTERS_CONF_ID, true, false, TEXT)
                .add(DATA_ADAPTER_NAME, true, false, TEXT)
                .add(ITEM_TEMPLATE, true, true, TEXT)
                .add(TOPIC_MAPPING, true, true, MAP_SUFFIX, TEXT)
                .add(FIELD_MAPPING, true, true, TEXT)
                .add(KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                .add(VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                .add(KEY_EVALUATOR_TYPE, false, false, TEXT, defaultValue("RAW"))
                .add(KEY_SCHEMA_FILE, false, false, TEXT)
                .add(VALUE_EVALUATOR_TYPE, false, false, TEXT, defaultValue("RAW"))
                .add(VALUE_SCHEMA_FILE, false, false, TEXT)
                .add(ITEM_INFO_NAME, false, false, TEXT, defaultValue("INFO"))
                .add(ITEM_INFO_FIELD, false, false, TEXT, defaultValue("MSG"))
                .add(CONSUMER_AUTO_OFFSET_RESET_CONFIG, false, false, TEXT, defaultValue("latest"))
                .add(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, false, false, BOOL, false, defaultValue("false"));

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

    public static ConnectorConfig newConfig(File adapterDir, Map<String, String> params) {
        return new ConnectorConfig(ConnectorConfig.appendAdapterDir(params, adapterDir));
    }

    private String get(String key, Type type, boolean forceRequired) {
        ConfParameter param = configSpec.getParameter(key);
        if (param.type().equals(type)) {
            if (param.required() && configuration.containsKey(key)) {
                return configuration.get(key);
            } else {
                String value = configuration.getOrDefault(key, param.defaultHolder().value(configuration));
                if (forceRequired && value == null) {
                    throw new ConfigException("Missing required parameter [%s]".formatted(key));
                }
                return value;
            }
        }
        throw new ConfigException(
                "No parameter [%s] of %s type is present in the configuration".formatted(key, type));

    }

    public String getText(String configKey) {
        return get(configKey, TEXT, false);
    }

    public String getBoolean(String configKey) {
        return get(configKey, BOOL, false);
    }

    public String getHost(String configKey) {
        return get(configKey, ConfType.Host, false);
    }

    public String getHost(String configKey, boolean forceRequired) {
        String value = get(configKey, ConfType.Host, false);
        if (forceRequired && value == null) {
            throw new ConfigException("Missing required parameter [%s]".formatted(configKey));
        }
        return value;
    }

    public String getUrl(String configKey, boolean forceRequired) {
        return get(configKey, URL, forceRequired);
    }

    public String getHostsList(String configKey) {
        return get(configKey, ConfType.HostsList, false);
    }

    public String getDirectory(String configKey) {
        return get(configKey, ConfType.Directory, false);
    }

    public Map<String, String> getValues(String configKey, boolean remap) {
        ConfParameter param = this.configSpec.getParameter(configKey);
        if (param.multiple()) {
            Map<String, String> newMap = new HashMap<>();
            for (Map.Entry<String, String> e : configuration.entrySet()) {
                if (remap) {
                    Optional<String> infix = ConfigSpec.extractInfix(param, e.getKey());
                    if (infix.isPresent()) {
                        newMap.put(infix.get(), e.getValue());
                    }
                } else {
                    if (e.getKey().startsWith(configKey)) {
                        newMap.put(e.getKey(), e.getValue());
                    }
                }

            }
            return newMap;
        }
        return Collections.emptyMap();
    }

    public <T> List<T> getAsList(String configKey, Function<? super Entry<String, String>, T> conv) {
        Map<String, String> values = getValues(configKey, true);
        return values.entrySet().stream().map(conv).toList();
    }

    public Properties baseConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, getText(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, getBoolean(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG));
        // properties.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "0");
        // properties.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "0");
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
}
