package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
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

public class ConnectorConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        Properties consumerProperties();

        Selectors<K, V> fieldsSelectors();

        ItemTemplates<K, V> itemTemplates();

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectorConfigurator.class);

    private final File adapterDir;

    public ConnectorConfigurator(File adapterDir) {
        this.adapterDir = adapterDir;
    }

    public ConsumerLoopConfig<?, ?> configure(Map<String, String> params) throws ConfigException {
        ConnectorConfig connectorConfig = new ConnectorConfig(ConnectorConfig.appendAdapterDir(params, adapterDir));
        Map<String, String> configuration = connectorConfig.configuration();

        // Process "map.<topic-name>.to"
        List<TopicMapping> topicMappings = connectorConfig.getAsList(ConnectorConfig.MAP,
                e -> new TopicMapping(e.getKey(), Arrays.asList(e.getValue())));

        // Process "field.<field-name>"
        Map<String, String> fieldsMapping = connectorConfig.getValues(ConnectorConfig.FIELD);

        SelectorsSupplier<?, ?> selectorsSupplier = SelectorsSupplier.wrap(
                makeKeySelectorSupplier(connectorConfig),
                makeValueSelectorSupplier(connectorConfig));

        Properties props = connectorConfig.baseConsumerProps();
        Deserializer<?> keyDeserializer = selectorsSupplier.keySelectorSupplier().deseralizer();
        Deserializer<?> valueDeserializer = selectorsSupplier.valueSelectorSupplier().deseralizer();

        ItemTemplates<?, ?> itemTemplates = initItemTemplates(selectorsSupplier, topicMappings);
        Selectors<?, ?> fieldsSelectors = Selectors.from(selectorsSupplier, "fields", fieldsMapping);

        return new DefaultConsumerLoopConfig(props, itemTemplates, fieldsSelectors, keyDeserializer, valueDeserializer);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            Properties consumerProperties, ItemTemplates<K, V> itemTemplates,
            Selectors<K, V> fieldsSelectors, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
            implements ConsumerLoopConfig<K, V> {
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

    private KeySelectorSupplier<?> makeKeySelectorSupplier(ConnectorConfig config) {
        String consumer = config.getText(ConnectorConfig.KEY_CONSUMER);
        return switch (consumer) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.keySelectorSupplier(config);
            case "JSON" -> JsonNodeSelectorsSuppliers.keySelectorSupplier(config);
            case "RAW" -> StringSelectorSuppliers.keySelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumer));
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(ConnectorConfig config) {
        String consumer = config.getText(ConnectorConfig.KEY_CONSUMER);
        return switch (consumer) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.valueSelectorSupplier(config);
            case "JSON" -> JsonNodeSelectorsSuppliers.valueSelectorSupplier(config);
            case "RAW" -> StringSelectorSuppliers.valueSelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumer));
        };
    }

}
