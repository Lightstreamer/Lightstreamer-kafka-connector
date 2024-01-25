package com.lightstreamer.kafka_connector.adapter;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Fields;
import com.lightstreamer.kafka_connector.adapter.mapping.Fields.FieldMappings;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;

public class ConnectorConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        Properties consumerProperties();

        FieldMappings<K, V> fieldMappings();

        ItemTemplates<K, V> itemTemplates();

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectorConfigurator.class);

    private final ConnectorConfig connectorConfig;

    private ConnectorConfigurator(ConnectorConfig config) {
        this.connectorConfig = config;
    }

    public static ConsumerLoopConfig<?,?> configure(ConnectorConfig config) {
        return new ConnectorConfigurator(config).configure();
    }

    private ConsumerLoopConfig<?, ?> configure() throws ConfigException {
        TopicsConfig topicsConfig = TopicsConfig.of(connectorConfig);

        // Process "field.<field-name>"
        Map<String, String> fieldsMapping = connectorConfig.getValues(ConnectorConfig.FIELD, false);

        try {
            SelectorsSupplier<?, ?> selectorsSupplier = SelectorsSupplier.wrap(
                    makeKeySelectorSupplier(connectorConfig),
                    makeValueSelectorSupplier(connectorConfig));

            Properties props = connectorConfig.baseConsumerProps();
            Deserializer<?> keyDeserializer = selectorsSupplier.keySelectorSupplier().deseralizer();
            Deserializer<?> valueDeserializer = selectorsSupplier.valueSelectorSupplier().deseralizer();

            ItemTemplates<?, ?> itemTemplates = initItemTemplates(selectorsSupplier, topicsConfig);
            FieldMappings<?, ?> fieldMappings = initFieldMappings(selectorsSupplier, fieldsMapping);

            return new DefaultConsumerLoopConfig(props, itemTemplates, fieldMappings, keyDeserializer,
                    valueDeserializer);
        } catch (Exception e) {
            throw new ConfigException(e.getMessage());
        }

    }

    private FieldMappings<?, ?> initFieldMappings(SelectorsSupplier<?, ?> selectorsSupplier,
            Map<String, String> fieldsMapping) {
        return initFieldMappingsHelper(selectorsSupplier, fieldsMapping);
    }

    private <K, V> FieldMappings<K, V> initFieldMappingsHelper(SelectorsSupplier<K, V> selectorsSupplier,
            Map<String, String> fieldsMapping) {
        return Fields.fieldMappingsFrom(fieldsMapping, selectorsSupplier);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            Properties consumerProperties, ItemTemplates<K, V> itemTemplates,
            FieldMappings<K, V> fieldMappings, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
            implements ConsumerLoopConfig<K, V> {
    }

    private ItemTemplates<?, ?> initItemTemplates(SelectorsSupplier<?, ?> selectorsSupplier,
            TopicsConfig topicsConfig) {
        return initItemTemplatesHelper(topicsConfig, selectorsSupplier);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(TopicsConfig topicsConfig,
            SelectorsSupplier<K, V> selectorsSupplier) {
        return Items.templatesFrom(topicsConfig, selectorsSupplier);
    }

    private KeySelectorSupplier<?> makeKeySelectorSupplier(ConnectorConfig config) {
        String consumer = config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE);
        return switch (consumer) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.keySelectorSupplier(config);
            case "JSON" -> JsonNodeSelectorsSuppliers.keySelectorSupplier(config);
            case "RAW" -> StringSelectorSuppliers.keySelectorSupplier();
            default -> throw new ConfigException("No available key evaluator %s".formatted(consumer));
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(ConnectorConfig config) {
        String consumer = config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        return switch (consumer) {
            case "AVRO" -> GenericRecordSelectorsSuppliers.valueSelectorSupplier(config);
            case "JSON" -> JsonNodeSelectorsSuppliers.valueSelectorSupplier(config);
            case "RAW" -> StringSelectorSuppliers.valueSelectorSupplier();
            default -> throw new ConfigException("No available value consumer %s".formatted(consumer));
        };
    }

}
