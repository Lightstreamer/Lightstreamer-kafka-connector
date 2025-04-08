
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

package com.lightstreamer.kafka.common.mapping;

import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemReference;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicConfiguration;
import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class Items {

    public interface Item extends SchemaAndValues {}

    public interface SubscribedItem extends Item {

        Object itemHandle();

        boolean isSnapshot();

        void setSnapshot(boolean flag);
    }

    /**
     * Represents a collection of {@link SubscribedItem} objects that can be iterated over. This
     * interface extends {@link Iterable}, allowing for iteration through the subscribed items.
     *
     * <p>Provides a static factory method {@code of} to create an instance of {@code
     * SubscribedItems} from a given {@link Collection} of {@link SubscribedItem}.
     */
    public interface SubscribedItems extends Iterable<SubscribedItem> {
        static SubscribedItems of(Collection<SubscribedItem> items) {
            return () -> items.iterator();
        }
    }

    public interface ItemTemplates<K, V> {

        boolean matches(Item item);

        Map<String, Set<DataExtractor<K, V>>> groupExtractors();

        // Only for testing purposes
        Set<Schema> getExtractorSchemasByTopicName(String topic);

        Set<String> topics();

        default boolean isRegexEnabled() {
            return false;
        }

        Optional<Pattern> subscriptionPattern();
    }

    private static class DefaultItem implements Item {

        private final Map<String, String> valuesMap;
        private final Schema schema;
        private final String str;

        DefaultItem(String prefix, Map<String, String> values) {
            this.valuesMap = values;
            this.schema = Schema.from(prefix, values.keySet());
            this.str =
                    String.format("(%s-<%s>), {%s}", prefix, values.toString(), schema.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(valuesMap, schema);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DefaultItem other
                    && Objects.equals(valuesMap, other.valuesMap)
                    && Objects.equals(schema, other.schema);
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public Map<String, String> values() {
            return valuesMap;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    private static class DefaultSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final DefaultItem wrappedItem;
        private boolean snapshotFlag;

        DefaultSubscribedItem(Object itemHandle, String prefix, Map<String, String> values) {
            this.wrappedItem = new DefaultItem(prefix, values);
            this.itemHandle = itemHandle;
            this.snapshotFlag = true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, wrappedItem);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DefaultSubscribedItem other
                    && wrappedItem.equals(other.wrappedItem)
                    && Objects.equals(itemHandle, other.itemHandle);
        }

        @Override
        public Schema schema() {
            return wrappedItem.schema;
        }

        @Override
        public Map<String, String> values() {
            return wrappedItem.values();
        }

        @Override
        public Object itemHandle() {
            return itemHandle;
        }

        @Override
        public boolean isSnapshot() {
            return snapshotFlag;
        }

        @Override
        public void setSnapshot(boolean flag) {
            this.snapshotFlag = flag;
        }

        @Override
        public String toString() {
            return wrappedItem.toString();
        }
    }

    private static class ItemTemplate<K, V> {

        private final Schema schema;
        private final String topic;
        private final DataExtractor<K, V> extractor;

        ItemTemplate(String topic, DataExtractor<K, V> extractor) {
            this.topic = Objects.requireNonNull(topic);
            this.extractor = Objects.requireNonNull(extractor);
            this.schema = extractor.schema();
        }

        public boolean matches(Item item) {
            return schema.matches(item.schema());
        }

        DataExtractor<K, V> selectors() {
            return extractor;
        }

        String topic() {
            return topic;
        }
    }

    private static class DefaultItemTemplates<K, V> implements ItemTemplates<K, V> {

        private final List<ItemTemplate<K, V>> templates;
        private final boolean regexEnabled;
        private final Optional<Pattern> pattern;

        DefaultItemTemplates(List<ItemTemplate<K, V>> templates, boolean regexEnabled) {
            this.templates = Collections.unmodifiableList(templates);
            this.regexEnabled = regexEnabled;
            this.pattern = makeOptionalPattern();
        }

        private Optional<Pattern> makeOptionalPattern() {
            if (regexEnabled) {
                return Optional.of(
                        Pattern.compile(
                                templates.stream()
                                        .map(t -> "(?:%s)".formatted(t.topic()))
                                        .distinct()
                                        .sorted() // Only helps to simplify unit tests
                                        .collect(joining("|"))));
            }
            return Optional.empty();
        }

        @Override
        public boolean matches(Item item) {
            return templates.stream().anyMatch(i -> i.matches(item));
        }

        @Override
        public Map<String, Set<DataExtractor<K, V>>> groupExtractors() {
            return templates.stream()
                    .collect(
                            groupingBy(
                                    ItemTemplate::topic,
                                    mapping(ItemTemplate::selectors, toSet())));
        }

        @Override
        public Set<String> topics() {
            return templates.stream().map(ItemTemplate::topic).collect(toSet());
        }

        @Override
        public Set<Schema> getExtractorSchemasByTopicName(String topic) {
            return groupExtractors().getOrDefault(topic, emptySet()).stream()
                    .map(DataExtractor::schema)
                    .collect(toSet());
        }

        @Override
        public boolean isRegexEnabled() {
            return regexEnabled;
        }

        @Override
        public Optional<Pattern> subscriptionPattern() {
            return pattern;
        }

        @Override
        public String toString() {
            return templates.stream().map(Object::toString).collect(joining(","));
        }
    }

    public static SubscribedItem subscribedFrom(String input) throws ExpressionException {
        return subscribedFrom(input, input);
    }

    public static SubscribedItem subscribedFrom(String input, Object itemHandle)
            throws ExpressionException {
        SubscriptionExpression result = Expressions.Subscription(input);
        return subscribedFrom(itemHandle, result.prefix(), result.params());
    }

    public static SubscribedItem subscribedFrom(
            Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultSubscribedItem(itemHandle, prefix, values);
    }

    public static Item itemFrom(String prefix, Map<String, String> values) {
        return new DefaultItem(prefix, values);
    }

    public static <K, V> ItemTemplates<K, V> templatesFrom(
            TopicConfigurations topicsConfig, KeyValueSelectorSuppliers<K, V> sSuppliers)
            throws ExtractionException {
        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicConfiguration topicConfig : topicsConfig.configurations()) {
            for (ItemReference reference : topicConfig.itemReferences()) {
                DataExtractor<K, V> dataExtractor =
                        reference.isTemplate()
                                ? extractor(sSuppliers, reference.template())
                                : extractor(sSuppliers, reference.itemName());
                templates.add(new ItemTemplate<>(topicConfig.topic(), dataExtractor));
            }
        }
        return new DefaultItemTemplates<>(templates, topicsConfig.isRegexEnabled());
    }

    private Items() {}
}
