
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

import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs.empty;
import static com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig.fromDelimitedMappings;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Avro;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.AvroKeyJsonValue;

import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemTemplatesUtils {

    public static <K, V> ItemTemplates<K, V> ItemTemplates(
            KeyValueSelectorSuppliers<K, V> sSuppliers, List<String> topics, List<String> templates)
            throws ExtractionException {

        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        Map<String, String> templatesMap = new HashMap<>();
        for (int i = 0; i < templates.size(); i++) {
            // Add a new item template with name "template-<i>"
            templatesMap.put("template-" + i, templates.get(i));
        }

        for (String topic : topics) {
            // Add a new TopicMapping referencing the template
            topicMappings.add(
                    fromDelimitedMappings(topic, getFullTemplateNames(templatesMap.keySet())));
        }

        return mkItemTemplates(sSuppliers, topicMappings, ItemTemplateConfigs.from(templatesMap));
    }

    /** Map the specified topic to the specified item names */
    public static <K, V> ItemTemplates<K, V> mkSimpleItems(
            KeyValueSelectorSuppliers<K, V> sSuppliers, List<String> topics, List<String> items)
            throws ExtractionException {
        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        String delimitedItems = String.join(",", items);
        for (String topic : topics) {
            // Add a new TopicMapping referencing the item name
            topicMappings.add(fromDelimitedMappings(topic, delimitedItems));
        }

        // No template configuration required in this case
        return mkItemTemplates(sSuppliers, topicMappings, empty());
    }

    public static ItemTemplates<GenericRecord, GenericRecord> AvroAvroTemplates(
            String topic, String template) throws ExtractionException {
        return ItemTemplates(Avro(), List.of(topic), List.of(template));
    }

    public static ItemTemplates<GenericRecord, JsonNode> AvroJsonTemplates(
            String topic, String template) throws ExtractionException {
        return ItemTemplates(AvroKeyJsonValue(), List.of(topic), List.of(template));
    }

    /*
     * Prefix each input element with "item-template." and concatenate all of them, separated by a comma character
     */
    static String getFullTemplateNames(Collection<String> template) {
        return template.stream().map(t -> "item-template." + t).collect(joining(","));
    }

    private static <K, V> ItemTemplates<K, V> mkItemTemplates(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            List<TopicMappingConfig> topicMappings,
            ItemTemplateConfigs templateConfigs)
            throws ExtractionException {
        TopicConfigurations topicsConfig = TopicConfigurations.of(templateConfigs, topicMappings);
        return Items.templatesFrom(topicsConfig, sSuppliers);
    }

    public static ItemTemplates<String, String> itemTemplates(String topic, String items) {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(empty(), List.of(fromDelimitedMappings(topic, items)));
        try {
            return Items.templatesFrom(topicsConfig, String());
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataExtractor<String, String> fieldsExtractor() {
        try {
            return FieldConfigs.from(Map.of("field", "#{VALUE}")).extractor(String(), false, false);
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }
}
