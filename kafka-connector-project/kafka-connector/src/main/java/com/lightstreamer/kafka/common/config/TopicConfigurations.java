
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves and holds the fully assembled topic-to-template configuration for the connector.
 *
 * <p>Combines {@link ItemTemplateConfigs} (named template definitions) with {@link
 * TopicMappingConfig} entries (topic-to-template bindings) into a set of {@link TopicConfiguration}
 * records, each pairing a topic with its resolved {@link TemplateExpression} list.
 *
 * <p>Example configuration:
 *
 * <pre>{@code
 * <param name="item-template.stock">stock-#{symbol=KEY.symbol}</param>
 * <param name="map.stocks.to">item-template.stock,simple-item-1</param>
 * <param name="map.users.to">item-template.user</param>
 * }</pre>
 *
 * @see TopicMappingConfig
 * @see ItemTemplateConfigs
 * @see TopicConfiguration
 */
public class TopicConfigurations {

    /**
     * Associates a Kafka topic name with a set of item template references or simple item names.
     *
     * <p>Each instance binds a single topic (literal or regex pattern) to one or more mappings,
     * where each mapping is either a reference to a named item template (e.g., {@code
     * "item-template.stock"}) or a plain item name (e.g., {@code "simple-item-1"}).
     *
     * <p>Example configuration:
     *
     * <pre>{@code
     * <param name="map.stocks.to">item-template.stock,simple-item-1</param>
     * }</pre>
     *
     * @see ItemTemplateConfigs
     * @see TopicConfiguration
     */
    public static class TopicMappingConfig {

        private final String topic;
        private final Set<String> mappings;
        private final Set<Integer> partitions;

        private TopicMappingConfig(String topic, Set<String> mappings, Set<Integer> partitions)
                throws ConfigException {
            this.topic = checkAndTrimTopic(topic);
            this.mappings = checkAndTrimMappings(mappings);
            this.partitions = partitions;
        }

        private String checkAndTrimTopic(String topic) {
            if (topic == null || topic.isBlank()) {
                throw new ConfigException("Topic must be a non-empty string");
            }
            return topic.trim();
        }

        private Set<String> checkAndTrimMappings(Set<String> mappings) throws ConfigException {
            LinkedHashSet<String> trimmed = new LinkedHashSet<>();
            for (String mapping : mappings) {
                if (mapping.isBlank()) {
                    throw new ConfigException("Topic mappings must be non-empty strings");
                }
                trimmed.add(mapping.trim());
            }
            return Collections.unmodifiableSet(trimmed);
        }

        /**
         * Returns the Kafka topic name.
         *
         * @return the topic name (non-blank, trimmed)
         */
        public String topic() {
            return topic;
        }

        /**
         * Returns the item names and item-template references bound to this topic.
         *
         * @return an unmodifiable {@link Set} of the mappings for this topic
         */
        public Set<String> mappings() {
            return mappings;
        }

        /**
         * Returns the partitions to consume from for this topic.
         *
         * @return an unmodifiable {@link Set} of partition numbers, or an empty set if all
         *     partitions of the topic are to be consumed
         */
        public Set<Integer> partitions() {
            return partitions;
        }

        /**
         * Parses a comma-separated list of non-negative partition numbers and inclusive ranges into
         * a set of partition numbers.
         *
         * <p>Each comma-separated token is either a single non-negative integer (e.g. {@code "5"})
         * or a range in the form {@code start-end} with {@code 0 <= start <= end} (e.g. {@code
         * "4-6"}). Whitespace around numbers and hyphens is tolerated; duplicate and overlapping
         * ranges are coalesced.
         *
         * @param delimitedPartitions the delimited list to parse, or {@code null}/blank for an
         *     empty result
         * @return an unmodifiable {@link Set} of partition numbers in first-seen order, or an empty
         *     set if the input is {@code null} or blank
         * @throws ConfigException if any token contains a negative number, has a start greater than
         *     its end, or is not a valid integer or range
         */
        static Set<Integer> parsePartitionRanges(String delimitedPartitions)
                throws ConfigException {
            if (delimitedPartitions == null || delimitedPartitions.isBlank()) {
                return Collections.emptySet();
            }
            Set<Integer> partitions = new LinkedHashSet<>();
            List<String> partitionRanges = Split.byComma(delimitedPartitions);
            for (String range : partitionRanges) {
                // A leading '-' would otherwise slip through as an empty first bound and
                // surface as the misleading "bounds must be integers" error.
                if (range.trim().startsWith("-")) {
                    throw new ConfigException(
                            "Partition numbers must be non-negative: [" + range + "]");
                }
                List<String> bounds = Split.bySeparator('-', range);
                try {
                    int start = Integer.parseInt(bounds.get(0).trim());
                    partitions.add(start);
                    if (bounds.size() > 1) {
                        int end = Integer.parseInt(bounds.get(1).trim());
                        if (start > end) {
                            throw new ConfigException(
                                    "Partition range start must be <= end: [" + range + "]");
                        }
                        for (int i = start + 1; i <= end; i++) {
                            partitions.add(i);
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new ConfigException(
                            "Partition range bounds must be integers: [" + range + "]", e);
                }
            }
            return Collections.unmodifiableSet(partitions);
        }

        /**
         * Creates a {@code TopicMappingConfig} for the given topic by parsing comma-separated
         * mapping strings and a partition-ranges expression.
         *
         * @param topic the Kafka topic name (non-blank)
         * @param delimitedMappings comma-separated item names and/or item-template references (e.g.
         *     {@code "item-template.stock,simple-item-1"})
         * @param delimitedPartitions comma-separated partition numbers and inclusive ranges (e.g.
         *     {@code "0,2,4-6"}), or {@code null}/blank to consume from all partitions
         * @return a new {@code TopicMappingConfig}
         * @throws ConfigException if {@code topic} is blank, any mapping is blank, or the
         *     partitions expression is not a valid list of non-negative integers and ranges
         */
        public static TopicMappingConfig fromDelimitedMappings(
                String topic, String delimitedMappings, String delimitedPartitions)
                throws ConfigException {
            return new TopicMappingConfig(
                    topic,
                    new LinkedHashSet<>(Split.byComma(delimitedMappings)),
                    parsePartitionRanges(delimitedPartitions));
        }

        /**
         * Convenience overload of {@link #from(Map, Map)} with no partition mappings.
         *
         * @param topicToItems map from topic name to its comma-separated mapping string
         * @return a list of {@code TopicMappingConfig}, one per entry
         */
        public static List<TopicMappingConfig> from(Map<String, String> topicToItems) {
            return from(topicToItems, Collections.emptyMap());
        }

        /**
         * Creates a {@code TopicMappingConfig} list by joining a topic-to-items map with a
         * topic-to-partitions map. Every topic appearing in {@code partitionToItems} must also
         * appear in {@code topicToItems}, otherwise a {@link ConfigException} is thrown.
         *
         * @param topicToItems map from topic name to its comma-separated mapping string
         * @param partitionToItems map from topic name to its partition-ranges expression; topics
         *     that do not appear here are configured to consume from all partitions
         * @return a list of {@code TopicMappingConfig}, one per entry of {@code topicToItems}
         * @throws ConfigException if {@code partitionToItems} references a topic missing from
         *     {@code topicToItems}, or if any individual mapping is malformed (see {@link
         *     #fromDelimitedMappings(String, String, String)})
         */
        public static List<TopicMappingConfig> from(
                Map<String, String> topicToItems, Map<String, String> partitionToItems)
                throws ConfigException {
            Set<String> mappedTopics = new HashSet<>(topicToItems.keySet());
            Set<String> referencesTopics = new HashSet<>(partitionToItems.keySet());
            if (!mappedTopics.containsAll(referencesTopics)) {
                Set<String> missing = new HashSet<>(referencesTopics);
                missing.removeAll(mappedTopics);
                throw new ConfigException(
                        "Partition mappings found for topics with no item mappings: "
                                + missing
                                + "");
            }
            List<TopicMappingConfig> configs = new ArrayList<>();
            for (Map.Entry<String, String> entry : topicToItems.entrySet()) {
                String topic = entry.getKey();
                String mappings = entry.getValue();
                String partitions = "";
                if (partitionToItems.containsKey(topic)) {
                    partitions = partitionToItems.get(topic);
                }
                configs.add(fromDelimitedMappings(topic, mappings, partitions));
            }
            return configs;
        }
    }

    /**
     * Holds named item template definitions, each mapping a template name to a {@link
     * TemplateExpression} that defines a schema prefix and extraction expressions.
     *
     * <p>A template expression uses the syntax {@code PREFIX-#{param1=EXPR1,param2=EXPR2}}, where
     * each parameter key becomes part of the item's {@link
     * com.lightstreamer.kafka.common.mapping.selectors.Schema} and each expression is evaluated
     * against incoming Kafka records to build canonical item names.
     *
     * <p>Example configuration:
     *
     * <pre>{@code
     * <param name="item-template.stock">stock-#{symbol=KEY.symbol}</param>
     * <param name="item-template.user">user-#{userId=VALUE.id,accountId=VALUE.accountId}</param>
     * }</pre>
     *
     * @see TopicMappingConfig
     * @see TemplateExpression
     */
    public static final class ItemTemplateConfigs {

        private static final ItemTemplateConfigs EMPTY = new ItemTemplateConfigs();

        private final Map<String, TemplateExpression> templates = new HashMap<>();

        private ItemTemplateConfigs() throws ConfigException {
            this(Collections.emptyMap());
        }

        private ItemTemplateConfigs(Map<String, String> configs) throws ConfigException {
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

        /**
         * Creates an {@code ItemTemplateConfigs} from a map of template name to template
         * expression.
         *
         * @param configs map from template name (e.g. {@code "stock"}) to its template expression
         *     (e.g. {@code "stock-#{symbol=KEY.symbol}"})
         * @return a new {@code ItemTemplateConfigs} holding the parsed {@link TemplateExpression}s
         * @throws ConfigException if any template expression is malformed
         */
        public static ItemTemplateConfigs from(Map<String, String> configs) throws ConfigException {
            return new ItemTemplateConfigs(configs);
        }

        /**
         * Returns the shared empty {@code ItemTemplateConfigs} instance (no templates defined).
         *
         * @return the shared empty {@code ItemTemplateConfigs}
         */
        public static ItemTemplateConfigs empty() {
            return EMPTY;
        }

        /**
         * Returns a defensive copy of the named template map.
         *
         * @return a new {@link Map} from template name to its {@link TemplateExpression}
         */
        public Map<String, TemplateExpression> templates() {
            return new HashMap<>(templates);
        }

        /**
         * Checks whether a template with the given name is defined.
         *
         * @param templateName the template name to look up
         * @return {@code true} if a template with the given name is defined, {@code false}
         *     otherwise
         */
        public boolean contains(String templateName) {
            return templates.containsKey(templateName);
        }

        /**
         * Returns the {@link TemplateExpression} for the given template name.
         *
         * @param templateName the template name to look up
         * @return the {@code TemplateExpression} for the given name, or {@code null} if no such
         *     template is defined
         */
        public TemplateExpression getTemplateExpression(String templateName) {
            return templates.get(templateName);
        }
    }

    /**
     * A single fully-resolved topic-to-templates binding: a Kafka topic name (literal or regex
     * pattern), the list of {@link TemplateExpression}s that records from that topic are matched
     * against, and the (optionally restricted) set of partitions to consume from.
     *
     * @see TopicMappingConfig
     * @see ItemTemplateConfigs
     * @param topic the Kafka topic name (literal or regex pattern, depending on {@link
     *     TopicConfigurations#isRegexEnabled()})
     * @param itemReferences the {@code TemplateExpression}s bound to this topic
     * @param partitions the set of partitions to consume from, or an empty set to consume from all
     *     partitions of the topic
     */
    public static record TopicConfiguration(
            String topic, List<TemplateExpression> itemReferences, Set<Integer> partitions) {}

    private final Set<TopicConfiguration> topicConfigurations;
    private final boolean regexEnabled;

    private TopicConfigurations(
            ItemTemplateConfigs itemTemplateConfigs,
            List<TopicMappingConfig> topicMappingConfigs,
            boolean regexEnabled)
            throws ConfigException {
        Set<TopicConfiguration> configs = new LinkedHashSet<>();
        for (TopicMappingConfig topicMapping : topicMappingConfigs) {
            List<TemplateExpression> refs =
                    topicMapping.mappings().stream()
                            .map(itemRef -> getTemplateExpression(itemRef, itemTemplateConfigs))
                            .toList();
            configs.add(
                    new TopicConfiguration(topicMapping.topic(), refs, topicMapping.partitions()));
        }
        topicConfigurations = Collections.unmodifiableSet(configs);
        this.regexEnabled = regexEnabled;
    }

    /**
     * Convenience overload of {@link #of(ItemTemplateConfigs, List, boolean)} with regex topic
     * matching disabled.
     *
     * @param itemTemplateConfigs the named template definitions
     * @param topicMappingConfigs the topic-to-template bindings
     * @return a new {@code TopicConfigurations} with regex disabled
     * @throws ConfigException if any topic mapping references an item template that is not defined
     *     in {@code itemTemplateConfigs}
     */
    public static TopicConfigurations of(
            ItemTemplateConfigs itemTemplateConfigs, List<TopicMappingConfig> topicMappingConfigs)
            throws ConfigException {
        return of(itemTemplateConfigs, topicMappingConfigs, false);
    }

    /**
     * Resolves the given item template definitions and topic mappings into a {@code
     * TopicConfigurations}.
     *
     * @param itemTemplateConfigs the named template definitions
     * @param topicMappingConfigs the topic-to-template bindings
     * @param regexEnabled {@code true} to treat each topic name as a regular-expression pattern,
     *     {@code false} to treat it as a literal
     * @return a new {@code TopicConfigurations}
     * @throws ConfigException if any topic mapping references an item template that is not defined
     *     in {@code itemTemplateConfigs}
     */
    public static TopicConfigurations of(
            ItemTemplateConfigs itemTemplateConfigs,
            List<TopicMappingConfig> topicMappingConfigs,
            boolean regexEnabled)
            throws ConfigException {
        return new TopicConfigurations(itemTemplateConfigs, topicMappingConfigs, regexEnabled);
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

    /**
     * Returns the fully-resolved topic configurations.
     *
     * @return an unmodifiable {@link Set} of {@link TopicConfiguration} records
     */
    public Set<TopicConfiguration> configurations() {
        return topicConfigurations;
    }
}
