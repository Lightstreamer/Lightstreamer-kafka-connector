
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
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka_connector.adapters.mapping.Fields;
import com.lightstreamer.kafka_connector.adapters.mapping.Items;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.Selected;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.string.StringSelectorSuppliers;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConsumerLoopConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        String connectionName();

        Properties consumerProperties();

        Selectors<K, V> fieldSelectors();

        ItemTemplates<K, V> itemTemplates();

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();

        RecordErrorHandlingStrategy recordErrorHandlingStrategy();
    }

    private final ConnectorConfig connectorConfig;

    private final Logger log;

    private ConsumerLoopConfigurator(ConnectorConfig config) {
        this.connectorConfig = config;
        this.log = LoggerFactory.getLogger(config.getAdapterName());
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
            Selected<?, ?> selected =
                    Selected.with(
                            makeKeySelectorSupplier(connectorConfig),
                            makeValueSelectorSupplier(connectorConfig));

            Properties props = connectorConfig.baseConsumerProps();
            Deserializer<?> keyDeserializer = selected.keySelectorSupplier().deseralizer();
            Deserializer<?> valueDeserializer = selected.valueSelectorSupplier().deseralizer();

            ItemTemplates<?, ?> itemTemplates = initItemTemplates(selected, topicsConfig);
            Selectors<?, ?> fieldSelectors = initFieldSelectors(selected, fieldsMapping);

            return new DefaultConsumerLoopConfig(
                    connectorConfig.getAdapterName(),
                    props,
                    itemTemplates,
                    fieldSelectors,
                    keyDeserializer,
                    valueDeserializer,
                    connectorConfig.getRecordExtractionErrorHandlingStrategy());
        } catch (Exception e) {
            log.atError().setCause(e).log();
            throw new ConfigException(e.getMessage());
        }
    }

    private Selectors<?, ?> initFieldSelectors(
            Selected<?, ?> selected, Map<String, String> fieldsMapping) {
        return initFieldSelectorsHelper(selected, fieldsMapping);
    }

    private <K, V> Selectors<K, V> initFieldSelectorsHelper(
            Selected<K, V> selected, Map<String, String> fieldsMapping) {
        return Fields.fromMapping(fieldsMapping, selected);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            Selectors<K, V> fieldSelectors,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            RecordErrorHandlingStrategy recordErrorHandlingStrategy)
            implements ConsumerLoopConfig<K, V> {}

    private ItemTemplates<?, ?> initItemTemplates(
            Selected<?, ?> selected, TopicsConfig topicsConfig) {
        return initItemTemplatesHelper(topicsConfig, selected);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(
            TopicsConfig topicsConfig, Selected<K, V> selected) {
        return Items.templatesFrom(topicsConfig, selected);
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
