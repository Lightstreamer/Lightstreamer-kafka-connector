
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
import java.util.Iterator;
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
     * Represents a collection of subscribed items.
     *
     * <p>This interface extends {@link Iterable} to allow iterating over the contained {@link
     * SubscribedItem} elements. It also provides factory methods to create instances from
     * collections or to obtain a no-operation implementation. through a collection of {@link
     * SubscribedItem} objects.
     *
     * @see SubscribedItem
     */
    public interface SubscribedItems extends Iterable<SubscribedItem> {

        static SubscribedItems of(Collection<SubscribedItem> sourceItems) {
            return new DefaultSubscribedItems(sourceItems, false);
        }

        static SubscribedItems of(
                Collection<SubscribedItem> sourceItems, boolean allowImplicitItems) {
            return new DefaultSubscribedItems(sourceItems, allowImplicitItems);
        }

        boolean allowImplicitItems();
    }

    private static class DefaultSubscribedItems implements SubscribedItems {

        private final Collection<SubscribedItem> sourceItems;
        private final boolean allowImplicitItems;

        DefaultSubscribedItems(Collection<SubscribedItem> sourceItems, boolean allowImplicitItems) {
            this.sourceItems = sourceItems;
            this.allowImplicitItems = allowImplicitItems;
        }

        @Override
        public Iterator<SubscribedItem> iterator() {
            return sourceItems.iterator();
        }

        @Override
        public boolean allowImplicitItems() {
            return allowImplicitItems;
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

    static class DefaultSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final SchemaAndValues schemaAndValues;
        private boolean snapshotFlag;

        DefaultSubscribedItem(SchemaAndValues schemaAndValues) {
            this(null, schemaAndValues);
        }

        DefaultSubscribedItem(Object itemHandle, SchemaAndValues schemaAndValues) {
            this.schemaAndValues = Objects.requireNonNull(schemaAndValues);
            this.itemHandle = itemHandle;
            this.snapshotFlag = true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, schemaAndValues);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DefaultSubscribedItem other
                    && Objects.equals(schemaAndValues, other.schemaAndValues)
                    && Objects.equals(itemHandle, other.itemHandle);
        }

        @Override
        public Schema schema() {
            return schemaAndValues.schema();
        }

        @Override
        public Map<String, String> values() {
            return schemaAndValues.values();
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
            return schemaAndValues.toString();
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

    public static SubscribedItem subscribedFrom(String input) throws ExpressionException {
        return subscribedFrom(input, input);
    }

    public static SubscribedItem subscribedFrom(String input, Object itemHandle)
            throws ExpressionException {
        SubscriptionExpression result = Expressions.Subscription(input);
        return subscribedFrom(itemHandle, result.prefix(), result.params());
    }

    public static SubscribedItem subscribedFrom(SchemaAndValues schemaAndValues)
            throws ExpressionException {
        return new DefaultSubscribedItem(schemaAndValues);
    }

    private static SubscribedItem subscribedFrom(
            Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultSubscribedItem(itemHandle, SchemaAndValues.from(prefix, values));
    }

    private Items() {}
}
