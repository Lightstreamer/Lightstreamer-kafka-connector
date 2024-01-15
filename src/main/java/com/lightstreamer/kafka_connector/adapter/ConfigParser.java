package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;

public class ConfigParser {

    public interface ConsumerLoopConfig<K, V> {

        Properties consumerProperties();

        Selectors<K, V> fieldsSelectors();

        ItemTemplates<K, V> itemTemplates();
    }

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);

    private final File adapterDir;

    public ConfigParser(File adapterDir) {
        this.adapterDir = adapterDir;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ConsumerLoopConfig<?, ?> parse(Map<String, String> params) throws ConfigException {
        ConnectorConfig connectorConfig = new ConnectorConfig(ConnectorConfig.appendAdapterDir(params, adapterDir));
        Map<String, String> configuration = connectorConfig.configuration();

        // Process "map.<topic-name>.to"
        List<TopicMapping> topicMappings = connectorConfig.getAsList(ConnectorConfig.MAP,
                e -> new TopicMapping(e.getKey(), Arrays.asList(new String[] { e.getValue() })));

        // Process "field.<field-name>"
        Map<String, String> fieldsMapping = connectorConfig.getValues(ConnectorConfig.FIELD);

        SelectorsSupplier<?, ?> selectorsSupplier = SelectorsSupplier.wrap(
                makeKeySelectorSupplier(configuration.get(ConnectorConfig.KEY_CONSUMER)),
                makeValueSelectorSupplier(configuration.get(ConnectorConfig.VALUE_CONSUMER)));

        Properties props = initProperties(configuration, connectorConfig, selectorsSupplier);
        ItemTemplates<?, ?> itemTemplates = initItemTemplates(selectorsSupplier, topicMappings);
        Selectors<?, ?> fieldsSelectors = Selectors.from(selectorsSupplier, "fields", fieldsMapping);

        return new DefaultConsumerLoopConfig(props, itemTemplates, fieldsSelectors);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            Properties consumerProperties, ItemTemplates<K, V> itemTemplates,
            Selectors<K, V> fieldsSelectors) implements ConsumerLoopConfig<K, V> {
    }

    private ItemTemplates<?, ?> initItemTemplates(SelectorsSupplier<?, ?> selectorsSupplier,
            List<TopicMapping> topicMappings)
            throws ConfigException {
        return initItemTemplatesHelper(topicMappings, selectorsSupplier);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(List<TopicMapping> topicMappings,
            SelectorsSupplier<K, V> selectorsSupplier)
            throws ConfigException {
        try {
            return Items.templatesFrom(topicMappings, selectorsSupplier);
        } catch (ExpressionException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    private Properties initProperties(Map<String, String> config, ConnectorConfig connectorConfig,
            SelectorsSupplier<?, ?> selectorsSupplier) {
        return initPropertiesHelper(config, connectorConfig, selectorsSupplier);
    }

    private <K, V> Properties initPropertiesHelper(Map<String, String> config, ConnectorConfig connectorConfig,
            SelectorsSupplier<K, V> selectorsSupplier) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectorConfig.getText(ConnectorConfig.BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, connectorConfig.getText(ConnectorConfig.GROUP_ID));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("adapter.dir", connectorConfig.getText(ConnectorConfig.ADAPTER_DIR));
        Optional.ofNullable(config.get("key.schema.file"))
                .ifPresent(v -> properties.setProperty("key.schema.file", v));
        Optional.ofNullable(config.get("value.schema.file"))
                .ifPresent(v -> properties.setProperty("value.schema.file", v));
        selectorsSupplier.keySelectorSupplier().config(config, properties);
        selectorsSupplier.valueSelectorSupplier().config(config, properties);
        return properties;
    }

    <K, V> ConsumerLoopConfig<K, V> loopConfig(Properties props, ItemTemplates<K, V> it, Selectors<K, V> f) {
        return new DefaultConsumerLoopConfig<>(props, it, f);
    }

    private KeySelectorSupplier<?> makeKeySelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.keySelectorSupplier();
            case "JSON" -> JsonNodeSelectorsSuppliers.keySelectorSupplier();
            case "RAW" -> StringSelectorSuppliers.keySelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.valueSelectorSupplier();
            case "JSON" -> JsonNodeSelectorsSuppliers.valueSelectorSupplier();
            case "RAW" -> StringSelectorSuppliers.valueSelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

}
