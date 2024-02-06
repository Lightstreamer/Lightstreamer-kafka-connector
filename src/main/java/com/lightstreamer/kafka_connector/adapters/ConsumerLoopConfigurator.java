
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

package com.lightstreamer.kafka_connector.adapters;

import com.lightstreamer.kafka_connector.adapters.config.ConfigException;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.Fields;
import com.lightstreamer.kafka_connector.adapters.mapping.Fields.FieldMappings;
import com.lightstreamer.kafka_connector.adapters.mapping.Items;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.string.StringSelectorSuppliers;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerLoopConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        Properties consumerProperties();

        FieldMappings<K, V> fieldMappings();

        ItemTemplates<K, V> itemTemplates();

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();

        String recordErrorHandlingStrategy();
    }

    private final Logger log;
    private final ConnectorConfig connectorConfig;

    private ConsumerLoopConfigurator(ConnectorConfig config) {
        this.connectorConfig = config;
        this.log =
                LoggerFactory.getLogger(
                        config.getText(ConnectorConfig.ADAPTERS_CONF_ID)
                                + "."
                                + this.getClass().getSimpleName());
    }

    public static ConsumerLoopConfig<?, ?> configure(ConnectorConfig config) {
        return new ConsumerLoopConfigurator(config).configure();
    }

    private ConsumerLoopConfig<?, ?> configure() throws ConfigException {
        TopicsConfig topicsConfig = TopicsConfig.of(connectorConfig);

        // Process "field.<field-name>"
        Map<String, String> fieldsMapping =
                connectorConfig.getValues(ConnectorConfig.FIELD_MAPPING, false);

        try {
            SelectorsSupplier<?, ?> selectorsSupplier =
                    SelectorsSupplier.wrap(
                            makeKeySelectorSupplier(connectorConfig),
                            makeValueSelectorSupplier(connectorConfig));

            Properties props = connectorConfig.baseConsumerProps();
            Deserializer<?> keyDeserializer = selectorsSupplier.keySelectorSupplier().deseralizer();
            Deserializer<?> valueDeserializer =
                    selectorsSupplier.valueSelectorSupplier().deseralizer();

            ItemTemplates<?, ?> itemTemplates = initItemTemplates(selectorsSupplier, topicsConfig);
            FieldMappings<?, ?> fieldMappings = initFieldMappings(selectorsSupplier, fieldsMapping);

            return new DefaultConsumerLoopConfig(
                    props,
                    itemTemplates,
                    fieldMappings,
                    keyDeserializer,
                    valueDeserializer,
                    "IGNORE_AND_CONTINUE");
        } catch (Exception e) {
            throw new ConfigException(e.getMessage());
        }
    }

    private FieldMappings<?, ?> initFieldMappings(
            SelectorsSupplier<?, ?> selectorsSupplier, Map<String, String> fieldsMapping) {
        return initFieldMappingsHelper(selectorsSupplier, fieldsMapping);
    }

    private <K, V> FieldMappings<K, V> initFieldMappingsHelper(
            SelectorsSupplier<K, V> selectorsSupplier, Map<String, String> fieldsMapping) {
        return Fields.fieldMappingsFrom(fieldsMapping, selectorsSupplier);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            FieldMappings<K, V> fieldMappings,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            String recordErrorHandlingStrategy)
            implements ConsumerLoopConfig<K, V> {}

    private ItemTemplates<?, ?> initItemTemplates(
            SelectorsSupplier<?, ?> selectorsSupplier, TopicsConfig topicsConfig) {
        return initItemTemplatesHelper(topicsConfig, selectorsSupplier);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(
            TopicsConfig topicsConfig, SelectorsSupplier<K, V> selectorsSupplier) {
        return Items.templatesFrom(topicsConfig, selectorsSupplier);
    }

    private KeySelectorSupplier<?> makeKeySelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluator = config.getEvaluator(ConnectorConfig.KEY_EVALUATOR_TYPE);
        return switch (evaluator) {
            case AVRO -> GenericRecordSelectorsSuppliers.keySelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.keySelectorSupplier(config);
            case STRING -> StringSelectorSuppliers.keySelectorSupplier();
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluatorType = config.getEvaluator(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        return switch (evaluatorType) {
            case AVRO -> GenericRecordSelectorsSuppliers.valueSelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.valueSelectorSupplier(config);
            case STRING -> StringSelectorSuppliers.valueSelectorSupplier();
        };
    }
}
