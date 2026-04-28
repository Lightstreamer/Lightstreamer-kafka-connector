
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
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.kvp.KvpSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Translates a {@link ConnectorConfig} into the {@link ConnectionSpec} consumed by the Kafka
 * consumer infrastructure.
 */
public class ConnectorConfigurator {

    private final ConnectorConfig config;

    /**
     * Creates a configurator by parsing raw adapter parameters.
     *
     * @param params the adapter parameter map
     * @param configDir the adapter configuration directory
     * @throws ConfigException if the configuration is invalid
     */
    public ConnectorConfigurator(Map<String, String> params, File configDir)
            throws ConfigException {
        this(ConnectorConfig.newConfig(configDir, params));
    }

    private ConnectorConfigurator(ConnectorConfig config) {
        this.config = config;
    }

    /**
     * Returns the underlying connector configuration.
     *
     * @return the {@link ConnectorConfig}
     */
    public ConnectorConfig getConfig() {
        return config;
    }

    /**
     * Builds a {@link ConnectionSpec} from the current configuration.
     *
     * @return the fully resolved {@code ConnectionSpec}
     * @throws ConfigException if the configuration cannot be resolved
     */
    public ConnectionSpec<?, ?> connectionSpec() throws ConfigException {
        try {
            return doConnectionSpec(config, mkKeyValueSelectorSuppliers(config));
        } catch (Exception e) {
            throw new ConfigException(e.getMessage(), e);
        }
    }

    private static <K, V> ConnectionSpec<K, V> doConnectionSpec(
            ConnectorConfig config, KeyValueSelectorSuppliers<K, V> sSuppliers)
            throws ExtractionException, ConfigException {
        FieldConfigs fieldConfigs = config.getFieldConfigs();

        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        config.getItemTemplateConfigs(),
                        config.getTopicMappings(),
                        config.isMapRegExEnabled());

        ItemTemplates<K, V> itemTemplates = Items.templatesFrom(topicsConfig, sSuppliers);
        FieldsExtractor<K, V> fieldsExtractor =
                fieldConfigs.fieldsExtractor(
                        sSuppliers,
                        config.isFieldsSkipFailedMappingEnabled(),
                        config.isFieldsMapNonScalarValuesEnabled());

        KafkaRecord.DeserializerPair<K, V> deserializerPair =
                new KafkaRecord.DeserializerPair<>(
                        sSuppliers.keySelectorSupplier().deserializer(),
                        sSuppliers.valueSelectorSupplier().deserializer());

        return new ConnectionSpec<>(
                config.getAdapterName(),
                config.baseConsumerProps(),
                itemTemplates,
                fieldsExtractor,
                deserializerPair,
                config.getRecordExtractionErrorHandlingStrategy(),
                CommandModeStrategy.from(
                        config.isAutoCommandModeEnabled(), config.isCommandEnforceEnabled()),
                new Concurrency(
                        config.getRecordConsumeWithOrderStrategy(),
                        config.getRecordConsumeWithNumThreads()));
    }

    static KeyValueSelectorSuppliers<?, ?> mkKeyValueSelectorSuppliers(ConnectorConfig config) {
        Map<EvaluatorType, KeyValueSelectorSuppliersMaker<?>> t = new HashMap<>();
        Function<? super EvaluatorType, ? extends KeyValueSelectorSuppliersMaker<?>> getMaker =
                type -> {
                    return switch (type) {
                        case JSON -> new JsonNodeSelectorsSuppliers(config);
                        case AVRO -> new GenericRecordSelectorsSuppliers(config);
                        case PROTOBUF -> new DynamicMessageSelectorSuppliers(config);
                        case KVP -> new KvpSelectorsSuppliers(config);
                        default -> new OthersSelectorSuppliers(config);
                    };
                };
        KeyValueSelectorSuppliersMaker<?> keyMaker =
                t.computeIfAbsent(config.getKeyEvaluator(), getMaker);
        KeyValueSelectorSuppliersMaker<?> valueMaker =
                t.computeIfAbsent(config.getValueEvaluator(), getMaker);

        return KeyValueSelectorSuppliers.of(
                keyMaker.makeKeySelectorSupplier(), valueMaker.makeValueSelectorSupplier());
    }
}
