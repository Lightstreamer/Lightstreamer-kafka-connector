
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

package com.lightstreamer.kafka.adapters;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.string.StringSelectorSuppliers;
import com.lightstreamer.kafka.config.ConfigException;
import com.lightstreamer.kafka.config.TopicsConfig;
import com.lightstreamer.kafka.mapping.Fields;
import com.lightstreamer.kafka.mapping.Items;
import com.lightstreamer.kafka.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConsumerLoopConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        String connectionName();

        Properties consumerProperties();

        ValuesExtractor<K, V> fieldsExtractor();

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
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        connectorConfig.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        connectorConfig.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        // Process "field.<field-name>=#{...}"
        Map<String, String> fieldsMapping =
                connectorConfig.getValues(ConnectorConfig.FIELD_MAPPING, true);

        try {
            SelectorSuppliers<?, ?> sSuppliers =
                    SelectorSuppliers.of(
                            mkKeySelectorSupplier(connectorConfig),
                            mkValueSelectorSupplier(connectorConfig));

            Properties props = connectorConfig.baseConsumerProps();
            ItemTemplates<?, ?> itemTemplates = initItemTemplates(sSuppliers, topicsConfig);
            ValuesExtractor<?, ?> fieldsExtractor = initFieldsExtractor(sSuppliers, fieldsMapping);

            return new DefaultConsumerLoopConfig(
                    connectorConfig.getAdapterName(),
                    props,
                    itemTemplates,
                    fieldsExtractor,
                    sSuppliers.keySelectorSupplier().deseralizer(),
                    sSuppliers.valueSelectorSupplier().deseralizer(),
                    connectorConfig.getRecordExtractionErrorHandlingStrategy());
        } catch (Exception e) {
            log.atError().setCause(e).log();
            throw new ConfigException(e.getMessage());
        }
    }

    private ValuesExtractor<?, ?> initFieldsExtractor(
            SelectorSuppliers<?, ?> selectorSuppliers, Map<String, String> fieldsMapping) {
        return initFieldExtractorHelper(selectorSuppliers, fieldsMapping);
    }

    private <K, V> ValuesExtractor<K, V> initFieldExtractorHelper(
            SelectorSuppliers<K, V> selectorSuppliers, Map<String, String> fieldsMapping) {
        return Fields.fromMapping(fieldsMapping, selectorSuppliers);
    }

    static record DefaultConsumerLoopConfig<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            ValuesExtractor<K, V> fieldsExtractor,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            RecordErrorHandlingStrategy recordErrorHandlingStrategy)
            implements ConsumerLoopConfig<K, V> {}

    private ItemTemplates<?, ?> initItemTemplates(
            SelectorSuppliers<?, ?> selected, TopicsConfig topicsConfig) {
        return initItemTemplatesHelper(topicsConfig, selected);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(
            TopicsConfig topicsConfig, SelectorSuppliers<K, V> selected) {
        return Items.templatesFrom(topicsConfig, selected);
    }

    private KeySelectorSupplier<?> mkKeySelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluatorType = config.getKeyEvaluator();
        return switch (evaluatorType) {
            case AVRO -> GenericRecordSelectorsSuppliers.keySelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.keySelectorSupplier(config);
            case STRING -> StringSelectorSuppliers.keySelectorSupplier();
            default -> OthersSelectorSuppliers.keySelectorSupplier(evaluatorType);
        };
    }

    private ValueSelectorSupplier<?> mkValueSelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluatorType = config.getValueEvaluator();
        return switch (evaluatorType) {
            case AVRO -> GenericRecordSelectorsSuppliers.valueSelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.valueSelectorSupplier(config);
            case STRING -> StringSelectorSuppliers.valueSelectorSupplier();
            default -> OthersSelectorSuppliers.valueSelectorSupplier(evaluatorType);
        };
    }
}
