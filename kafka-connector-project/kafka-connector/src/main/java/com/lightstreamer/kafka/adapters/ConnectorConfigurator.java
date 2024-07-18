
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
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValuesExtractor;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class ConnectorConfigurator {

    public interface ConsumerLoopConfig<K, V> {

        String connectionName();

        Properties consumerProperties();

        ValuesExtractor<K, V> fieldsExtractor();

        ItemTemplates<K, V> itemTemplates();

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();

        RecordErrorHandlingStrategy recordErrorHandlingStrategy();
    }

    private static record ConsumerLoopConfigImpl<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            ValuesExtractor<K, V> fieldsExtractor,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            RecordErrorHandlingStrategy recordErrorHandlingStrategy)
            implements ConsumerLoopConfig<K, V> {}

    private final ConnectorConfig config;
    private final Logger log;

    public ConnectorConfigurator(Map<String, String> params, File configDir)
            throws ConfigException {
        this(ConnectorConfig.newConfig(configDir, params));
    }

    private ConnectorConfigurator(ConnectorConfig config) {
        this.config = config;
        this.log = LoggerFactory.getLogger(config.getAdapterName());
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    protected ConsumerLoopConfig<?, ?> configure() throws ConfigException {
        // Process "field.<field-name>=#{...}"
        // Map<String, String> fieldsMapping = config.getFieldMappings();
        FieldConfigs fieldConfigs = config.getFieldConfigs();

        try {
            TopicConfigurations topicsConfig =
                    TopicConfigurations.of(
                            config.getItemTemplateConfigs(), config.getTopicMappings());
            SelectorSuppliers<?, ?> sSuppliers =
                    SelectorSuppliers.of(
                            mkKeySelectorSupplier(config), mkValueSelectorSupplier(config));

            ItemTemplates<?, ?> itemTemplates = initItemTemplates(sSuppliers, topicsConfig);
            ValuesExtractor<?, ?> fieldsExtractor = fieldConfigs.extractor(sSuppliers);
            // ValuesExtractor<?, ?> fieldsExtractor = initFieldsExtractor(sSuppliers,
            // fieldsMapping);

            return new ConsumerLoopConfigImpl(
                    config.getAdapterName(),
                    config.baseConsumerProps(),
                    itemTemplates,
                    fieldsExtractor,
                    sSuppliers.keySelectorSupplier().deseralizer(),
                    sSuppliers.valueSelectorSupplier().deseralizer(),
                    config.getRecordExtractionErrorHandlingStrategy());
        } catch (Exception e) {
            log.atError().setCause(e).log();
            throw new ConfigException(e.getMessage());
        }
    }

    private ValuesExtractor<?, ?> initFieldsExtractor(
            SelectorSuppliers<?, ?> selectorSuppliers, Map<String, String> fieldsMapping)
            throws ExtractionException {
        return initFieldExtractorHelper(selectorSuppliers, fieldsMapping);
    }

    private <K, V> ValuesExtractor<K, V> initFieldExtractorHelper(
            SelectorSuppliers<K, V> selectorSuppliers, Map<String, String> fieldsMapping)
            throws ExtractionException {
        // return FieldConfigs.extractorFrom(fieldsMapping, selectorSuppliers);
        return null;
    }

    private ItemTemplates<?, ?> initItemTemplates(
            SelectorSuppliers<?, ?> selected, TopicConfigurations topicsConfig)
            throws ExtractionException {
        return initItemTemplatesHelper(topicsConfig, selected);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(
            TopicConfigurations topicsConfig, SelectorSuppliers<K, V> selected)
            throws ExtractionException {
        return Items.from(topicsConfig, selected);
    }

    private KeySelectorSupplier<?> mkKeySelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluatorType = config.getKeyEvaluator();
        return switch (evaluatorType) {
            case AVRO -> GenericRecordSelectorsSuppliers.keySelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.keySelectorSupplier(config);
            default -> OthersSelectorSuppliers.keySelectorSupplier(evaluatorType);
        };
    }

    private ValueSelectorSupplier<?> mkValueSelectorSupplier(ConnectorConfig config) {
        EvaluatorType evaluatorType = config.getValueEvaluator();
        return switch (evaluatorType) {
            case AVRO -> GenericRecordSelectorsSuppliers.valueSelectorSupplier(config);
            case JSON -> JsonNodeSelectorsSuppliers.valueSelectorSupplier(config);
            default -> OthersSelectorSuppliers.valueSelectorSupplier(evaluatorType);
        };
    }
}
