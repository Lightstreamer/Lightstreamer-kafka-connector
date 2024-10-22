
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

package com.lightstreamer.kafka.test_utils;

import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.JSON;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Avro;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.AvroKeyJsonValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemTemplatesUtils {

    public static <K, V> ItemTemplates<K, V> ItemTemplates(
            String topic, KeyValueSelectorSuppliers<K, V> sSuppliers, String... template)
            throws ExtractionException {

        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        Map<String, String> templates = new HashMap<>();
        for (int i = 0; i < template.length; i++) {
            // Add a new item template with name "template-<i>"
            templates.put("template-" + i, template[i]);
            // Add a new TopicMapping referencing the template
            topicMappings.add(
                    TopicMappingConfig.fromDelimitedMappings(topic, "item-template.template-" + i));
        }

        return getTopicsConfig(sSuppliers, topicMappings, ItemTemplateConfigs.from(templates));
    }

    public static <K, V> ItemTemplates<K, V> mkSimpleItems(
            String topic, KeyValueSelectorSuppliers<K, V> sSuppliers, String... items)
            throws ExtractionException {
        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        for (int i = 0; i < items.length; i++) {
            // Add a new TopicMapping referencing the item name
            topicMappings.add(TopicMappingConfig.fromDelimitedMappings(topic, items[i]));
        }

        // No template configuration required in this case
        return getTopicsConfig(sSuppliers, topicMappings, ItemTemplateConfigs.empty());
    }

    private static <K, V> ItemTemplates<K, V> getTopicsConfig(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            List<TopicMappingConfig> topicMappings,
            ItemTemplateConfigs noTemplatesMap)
            throws ExtractionException {
        TopicConfigurations topicsConfig = TopicConfigurations.of(noTemplatesMap, topicMappings);
        return Items.templatesFrom(topicsConfig, sSuppliers);
    }

    public static ItemTemplates<GenericRecord, GenericRecord> AvroAvroTemplates(
            String topic, String template) throws ExtractionException {
        return ItemTemplates(topic, Avro(avroAvroConfig()), template);
    }

    public static ItemTemplates<GenericRecord, JsonNode> AvroJsonTemplates(
            String topic, String template) throws ExtractionException {
        return ItemTemplates(topic, AvroKeyJsonValue(avroJsonConfig()), template);
    }

    private static ConnectorConfig avroJsonConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        JSON.toString()));
    }

    private static ConnectorConfig avroAvroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc"));
    }
}
