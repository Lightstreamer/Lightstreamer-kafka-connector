
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

package com.lightstreamer.kafka.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TopicsConfig {

    public static record TopicConfiguration(String topic, ItemReference itemReference) {}

    public static class ItemReference {

        private String templateKey;
        private String templateValue;
        private String itemName;

        ItemReference(String itemTemplateKey, String itemTemplateValue) {
            this.templateKey = itemTemplateKey;
            this.templateValue = itemTemplateValue;
        }

        ItemReference(String itemName) {
            this.itemName = itemName;
        }

        public String templateKey() {
            return templateKey;
        }

        public String templateValue() {
            return templateValue;
        }

        public String itemName() {
            return itemName;
        }

        public boolean isTemplate() {
            return Objects.isNull(itemName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(templateKey, templateValue, itemName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof ItemReference other
                    && Objects.equals(templateKey, other.templateKey)
                    && Objects.equals(templateValue, other.templateValue)
                    && Objects.equals(itemName, other.itemName);
        }

        public static ItemReference forTemplate(String templateKey, String templateValue) {
            return new ItemReference(templateKey, templateValue);
        }

        public static ItemReference forSimpleName(String itemName) {
            return new ItemReference(itemName);
        }
    }

    private final List<TopicConfiguration> topicConfigurations;

    private TopicsConfig(TopicConfiguration... config) {
        this.topicConfigurations = List.of(config);
    }

    // private TopicsConfig(ConnectorConfig config) {
    //     this(
    //             config.getValues(ConnectorConfig.ITEM_TEMPLATE, false),
    //             config.getValues(ConnectorConfig.TOPIC_MAPPING, true));
    // }

    private TopicsConfig(Map<String, String> itemTemplates, Map<String, String> topicMappings) {
        this.topicConfigurations = initConfigurations(itemTemplates, topicMappings);
    }

    private List<TopicConfiguration> initConfigurations(
            Map<String, String> itemTemplates, Map<String, String> topicMappings) {
        List<TopicConfiguration> configs = new ArrayList<>();
        for (Map.Entry<String, String> topicMapping : topicMappings.entrySet()) {
            String topic = topicMapping.getKey();
            String[] itemRefs = topicMapping.getValue().split(",");
            for (String itemRef : itemRefs) {
                ItemReference itemReference = null;
                if (itemRef.startsWith("item-template.")) {
                    String templateName = itemRef.substring(itemRef.indexOf(".") + 1);
                    if (!itemTemplates.containsKey(templateName)) {
                        throw new ConfigException(
                                "No item template [%s] found".formatted(templateName));
                    }
                    String templateValue = itemTemplates.get(templateName);
                    itemReference = ItemReference.forTemplate(templateName, templateValue);
                } else {
                    itemReference = ItemReference.forSimpleName(itemRef);
                }
                configs.add(new TopicConfiguration(topic, itemReference));
            }
        }
        return configs.stream().distinct().toList();
    }

    public List<TopicConfiguration> configurations() {
        return topicConfigurations;
    }

    public static TopicsConfig of(TopicConfiguration... config) {
        return new TopicsConfig(config);
    }

    // public static TopicsConfig of(ConnectorConfig connectorConfig) {
    //     return new TopicsConfig(connectorConfig);
    // }

    public static TopicsConfig of(
            Map<String, String> itemTemplates, Map<String, String> topicMappings) {
        return new TopicsConfig(itemTemplates, topicMappings);
    }
}

