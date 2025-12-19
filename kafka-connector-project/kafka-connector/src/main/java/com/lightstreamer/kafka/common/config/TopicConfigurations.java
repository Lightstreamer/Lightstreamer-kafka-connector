
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.common.utils.Split;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
            return new TopicMappingConfig(
                    topic, new LinkedHashSet<>(Split.byComma(delimitedMappings)));
        }

        public static List<TopicMappingConfig> from(Map<String, String> configs) {
            return configs.entrySet().stream()
                    .map(e -> fromDelimitedMappings(e.getKey(), e.getValue()))
                    .toList();
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

        private final Map<String, TemplateExpression> templates = new HashMap<>();

        private ItemTemplateConfigs() {
            this(Collections.emptyMap());
        }

        private ItemTemplateConfigs(Map<String, String> configs) {
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String templateName = entry.getKey();
                String templateExpression = entry.getValue();
                try {
                    templates.put(templateName, Template(templateExpression));
                } catch (ExpressionException e) {
                    String msg =
                            "Got the following error while evaluating the template [%s] containing the expression [%s]: <%s>"
                                    .formatted(templateName, templateExpression, e.getMessage());
                    throw new ConfigException(msg);
                }
            }
        }

        public Map<String, TemplateExpression> templates() {
            return new HashMap<>(templates);
        }

        public boolean contains(String templateName) {
            return templates.containsKey(templateName);
        }

        public TemplateExpression getTemplateExpression(String templateName) {
            return templates.get(templateName);
        }
    }

    public static record TopicConfiguration(
            String topic, List<TemplateExpression> itemReferences) {}

    public static TopicConfigurations of(
            ItemTemplateConfigs itemTemplateConfigs, List<TopicMappingConfig> topicMappingConfigs) {
        return of(itemTemplateConfigs, topicMappingConfigs, false);
    }

    public static TopicConfigurations of(
            ItemTemplateConfigs itemTemplateConfigs,
            List<TopicMappingConfig> topicMappingConfigs,
            boolean regexEnabled) {
        return new TopicConfigurations(itemTemplateConfigs, topicMappingConfigs, regexEnabled);
    }

    private final Set<TopicConfiguration> topicConfigurations;
    private final boolean regexEnabled;

    private TopicConfigurations(
            ItemTemplateConfigs itemTemplateConfigs,
            List<TopicMappingConfig> topicMappingConfigs,
            boolean regexEnabled) {
        Set<TopicConfiguration> configs = new LinkedHashSet<>();
        for (TopicMappingConfig topicMapping : topicMappingConfigs) {
            List<TemplateExpression> refs =
                    topicMapping.mappings().stream()
                            .map(itemRef -> getTemplateExpression(itemRef, itemTemplateConfigs))
                            .toList();
            configs.add(new TopicConfiguration(topicMapping.topic(), refs));
        }
        this.topicConfigurations = Collections.unmodifiableSet(configs);
        this.regexEnabled = regexEnabled;
    }

    private TemplateExpression getTemplateExpression(
            String itemRef, ItemTemplateConfigs itemTemplateConfigs) {
        if (itemRef == null) {
            throw new IllegalArgumentException("itemRef is null");
        }
        if (itemRef.startsWith("item-template.")) {
            String templateName = itemRef.substring(itemRef.indexOf(".") + 1);
            if (templateName.isBlank()) {
                throw new ConfigException("Item template reference must be a non-empty string");
            }
            if (!itemTemplateConfigs.contains(templateName)) {
                throw new ConfigException("No item template [%s] found".formatted(templateName));
            }
            return itemTemplateConfigs.getTemplateExpression(templateName);
        }
        return Expressions.EmptyTemplate(itemRef);
    }

    public boolean isRegexEnabled() {
        return regexEnabled;
    }

    public Set<TopicConfiguration> configurations() {
        return topicConfigurations;
    }
}
