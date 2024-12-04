
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
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class ConnectorConfigurator {

    private static record ConsumerTriggerConfigImpl<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            DataExtractor<K, V> fieldsExtractor,
            KeyValueDeserializers<K, V> deserializers,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            Concurrency concurrency)
            implements ConsumerTriggerConfig<K, V> {}

    private static record ConcurrencyConfig(
            RecordConsumeWithOrderStrategy orderStrategy, int threads)
            implements ConsumerTriggerConfig.Concurrency {}

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

    public ConsumerTriggerConfig<?, ?> configure() throws ConfigException {
        try {
            return doConfigure(config, mkKeyValueSelectorSuppliers(config));
        } catch (Exception e) {
            log.atError().setCause(e).log();
            throw new ConfigException(e.getMessage());
        }
    }

    private static <K, V> ConsumerTriggerConfigImpl<K, V> doConfigure(
            ConnectorConfig config, WrapperKeyValueSelectorSuppliers<K, V> sSuppliers)
            throws ExtractionException {
        FieldConfigs fieldConfigs = config.getFieldConfigs();

        TopicConfigurations topicsConfig =
                TopicConfigurations.of(config.getItemTemplateConfigs(), config.getTopicMappings());

        ItemTemplates<K, V> itemTemplates = Items.templatesFrom(topicsConfig, sSuppliers);
        DataExtractor<K, V> fieldsExtractor = fieldConfigs.extractor(sSuppliers);

        return new ConsumerTriggerConfigImpl<>(
                config.getAdapterName(),
                config.baseConsumerProps(),
                itemTemplates,
                fieldsExtractor,
                sSuppliers.deserializers(),
                config.getRecordExtractionErrorHandlingStrategy(),
                new ConcurrencyConfig(
                        config.getRecordConsumeWithOrderStrategy(),
                        config.getRecordConsumeWithNumThreads()));
    }

    private static WrapperKeyValueSelectorSuppliers<?, ?> mkKeyValueSelectorSuppliers(
            ConnectorConfig config) {
        KeyValueSelectorSuppliersMaker<?> maker =
                switch (config.getKeyEvaluator()) {
                    case AVRO -> new GenericRecordSelectorsSuppliers(config);
                    case JSON -> new JsonNodeSelectorsSuppliers(config);
                    default -> new OthersSelectorSuppliers(config);
                };

        return new WrapperKeyValueSelectorSuppliers<>(
                maker.makeKeySelectorSupplier(), maker.makeValueSelectorSupplier());
    }
}
