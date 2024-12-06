
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

package com.lightstreamer.kafka.common.config;

import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;
import com.lightstreamer.kafka.common.utils.Either;
import com.lightstreamer.kafka.common.utils.Split;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicConfigurations {

    public static class TopicMappingConfig {

        private final String topic;
        private final Set<String> mappings;

        private TopicMappingConfig(String topic, LinkedHashSet<String> mappings) {
            this.topic = checkAndTrimTopic(topic);
            this.mappings = checkAndTrimMappings(mappings);
        }

        String checkAndTrimTopic(String topic) {
            if (topic == null || topic.isBlank()) {
                throw new ConfigException("Topic must be a non-empty string");
            }
            return topic;
        }

        public String topic() {
            return topic;
        }

        public Set<String> mappings() {
            return mappings;
        }

        private Set<String> checkAndTrimMappings(LinkedHashSet<String> mappings) {
            LinkedHashSet<String> trimmed = new LinkedHashSet<>();
            for (String mapping : mappings) {
                if (mapping.isBlank()) {
                    throw new ConfigException("Topic mappings must be non-empty strings");
                }
                trimmed.add(mapping.trim());
            }
            return Collections.unmodifiableSet(trimmed);
        }

        public static TopicMappingConfig fromDelimitedMappings(
                String topic, String delimitedMappings) {
            return from(topic, Split.byComma(delimitedMappings));
        }

        public static List<TopicMappingConfig> from(Map<String, String> configs) {
            return configs.entrySet().stream()
                    .map(e -> fromDelimitedMappings(e.getKey(), e.getValue()))
                    .toList();
        }

        private static TopicMappingConfig from(String topic, List<String> mapping) {
            return new TopicMappingConfig(topic, new LinkedHashSet<>(mapping));
        }
    }

    public static final class ItemTemplateConfigs {

        private static final ItemTemplateConfigs EMPTY = new ItemTemplateConfigs();

        public static ItemTemplateConfigs from(Map<String, String> configs) {
            return new ItemTemplateConfigs(configs);
        }

        public static ItemTemplateConfigs empty() {
            return EMPTY;
        }

        private final Map<String, TemplateExpression> expressions = new HashMap<>();

        private ItemTemplateConfigs() {
            this(Collections.emptyMap());
        }

        private ItemTemplateConfigs(Map<String, String> configs) {
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String templateName = entry.getKey();
                String templateExpression = entry.getValue();
                try {
                    TemplateExpression te = Expressions.Template(templateExpression);
                    expressions.put(templateName, te);
                } catch (ExpressionException e) {
                    String msg =
                            "Found the invalid expression [%s] while evaluating [%s]: <%s>"
                                    .formatted(templateExpression, templateName, e.getMessage());
                    throw new ConfigException(msg);
                }
            }
        }

        public Map<String, TemplateExpression> expressions() {
            return new HashMap<>(expressions);
        }

        public boolean contains(String templateName) {
            return expressions.containsKey(templateName);
        }

        public TemplateExpression getExpression(String templateName) {
            return expressions.get(templateName);
        }
    }

    public static final class ItemReference {

        public static ItemReference template(TemplateExpression result) {
            return new ItemReference(result);
        }

        public static ItemReference name(String itemName) {
            if (itemName == null || itemName.isBlank()) {
                throw new ConfigException("Item name must be a non-empty string");
            }
            return new ItemReference(itemName);
        }

        static ItemReference from(String itemRef, ItemTemplateConfigs itemTemplateConfigs) {
            if (itemRef == null) {
                throw new IllegalArgumentException("itemRef is null");
            }
            if (itemRef.startsWith("item-template.")) {
                String templateName = itemRef.substring(itemRef.indexOf(".") + 1);
                if (templateName.isBlank()) {
                    throw new ConfigException("Item template reference must be a non-empty string");
                }
                if (!itemTemplateConfigs.contains(templateName)) {
                    throw new ConfigException(
                            "No item template [%s] found".formatted(templateName));
                }
                TemplateExpression expression = itemTemplateConfigs.getExpression(templateName);
                return ItemReference.template(expression);
            }
            return ItemReference.name(itemRef);
        }

        private Either<String, TemplateExpression> item;

        private ItemReference(TemplateExpression result) {
            item = Either.right(result);
        }

        private ItemReference(String itemName) {
            item = Either.left(itemName);
        }

        public String itemName() {
            return item.getLeft();
        }

        public boolean isTemplate() {
            return item.isRight();
        }

        public TemplateExpression template() {
            return item.getRight();
        }

        @Override
        public int hashCode() {
            return Objects.hash(item);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof ItemReference other && Objects.equals(item, other.item);
        }
    }

    public static record TopicConfiguration(String topic, List<ItemReference> itemReferences) {}

    public static TopicConfigurations of(
            ItemTemplateConfigs itemTemplateConfigs, List<TopicMappingConfig> topicMappingConfigs) {
        return new TopicConfigurations(itemTemplateConfigs, topicMappingConfigs);
    }

    private final Set<TopicConfiguration> topicConfigurations;

    private TopicConfigurations(
            ItemTemplateConfigs itemTemplateConfigs, List<TopicMappingConfig> topicMappingConfigs) {
        Set<TopicConfiguration> configs = new LinkedHashSet<>();
        for (TopicMappingConfig topicMapping : topicMappingConfigs) {
            List<ItemReference> refs =
                    topicMapping.mappings().stream()
                            .map(itemRef -> ItemReference.from(itemRef, itemTemplateConfigs))
                            .toList();
            configs.add(new TopicConfiguration(topicMapping.topic(), refs));
        }
        this.topicConfigurations = Collections.unmodifiableSet(configs);
    }

    public Set<TopicConfiguration> configurations() {
        return topicConfigurations;
    }
}
