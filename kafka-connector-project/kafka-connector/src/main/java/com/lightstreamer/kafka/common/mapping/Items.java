
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

import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.canonicalItemExtractor;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicConfiguration;
import com.lightstreamer.kafka.common.mapping.selectors.CanonicalItemExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class Items {

    public interface Item {

        Schema schema();
    }

    public interface SubscribedItem extends Item {

        Object itemHandle();

        boolean isSnapshot();

        void setSnapshot(boolean flag);

        String asCanonicalItemName();
    }

    /**
     * An interface representing a collection of subscribed items that can be iterated over. This
     * collection maps item names to {@link SubscribedItem} objects and provides methods to add,
     * remove, and retrieve items.
     *
     * @see SubscribedItem
     */
    public interface SubscribedItems {

        static SubscribedItems of(Collection<SubscribedItem> items) {
            SubscribedItems subscribedItems = SubscribedItems.create();
            for (SubscribedItem item : items) {
                subscribedItems.addItem(item);
            }
            return subscribedItems;
        }

        /**
         * Creates a new empty instance of SubscribedItems.
         *
         * @return a new {@code SubscribedItems} instance
         */
        static SubscribedItems create() {
            return new DefaultSubscribedItems();
        }

        /**
         * Returns a no-operation implementation of {@code SubscribedItems}.
         *
         * @return a singleton no-operation implementation of the {@code SubscribedItems} interface
         */
        static SubscribedItems nop() {
            return NOPSubscribedItems.NOP;
        }

        /**
         * Determines whether subscriptions should be accepted.
         *
         * @return {@code true} if subscriptions are accepted, {@code false} otherwise
         */
        default boolean acceptSubscriptions() {
            return true;
        }

        /**
         * Adds a subscribed item to the collection.
         *
         * @param item the subscribed item to be added to the collection
         */
        void addItem(SubscribedItem item);

        /**
         * Removes a subscribed item identified by the given name.
         *
         * @param itemName the name of the item to remove
         * @return an {@code Optional} containing the removed {@link SubscribedItem} if it existed,
         *     or an empty Optional if no item with the given name was found
         */
        Optional<SubscribedItem> removeItem(String itemName);

        /**
         * Retrieves a subscribed item by its name.
         *
         * @param itemName the name of the item to retrieve
         * @return an {@code Optional} containing the {@link SubscribedItem} if found, or empty if
         *     no item with the given name exists
         */
        Optional<SubscribedItem> getItem(String itemName);

        /**
         * Checks if this collection of subscribed items is empty.
         *
         * @return {@code true} if this collection contains no subscribed items, {@code false}
         *     otherwise
         */
        boolean isEmpty();

        /**
         * Returns the number of subscribed items in this collection.
         *
         * @return the number of subscribed items
         */
        int size();

        /** Clears all subscribed items. */
        void clear();
    }

    private static class NOPSubscribedItems implements SubscribedItems {

        private static final NOPSubscribedItems NOP = new NOPSubscribedItems();

        private NOPSubscribedItems() {}

        @Override
        public boolean acceptSubscriptions() {
            return false; // No subscriptions are accepted in NOP mode
        }

        @Override
        public void addItem(SubscribedItem item) {
            // No operation
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            return Optional.empty();
        }

        @Override
        public Optional<SubscribedItem> getItem(String itemName) {
            return Optional.empty();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void clear() {
            // No operation
        }
    }

    private static class DefaultSubscribedItems implements SubscribedItems {

        private final Map<String, SubscribedItem> sourceItems;

        DefaultSubscribedItems() {
            this.sourceItems = new ConcurrentHashMap<>();
        }

        @Override
        public void addItem(SubscribedItem item) {
            sourceItems.put(item.asCanonicalItemName(), item);
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            return Optional.ofNullable(sourceItems.remove(itemName));
        }

        @Override
        public Optional<SubscribedItem> getItem(String itemName) {
            return Optional.ofNullable(sourceItems.get(itemName));
        }

        @Override
        public boolean isEmpty() {
            return sourceItems.isEmpty();
        }

        @Override
        public int size() {
            return sourceItems.size();
        }

        @Override
        public void clear() {
            sourceItems.clear();
        }
    }

    public interface ItemTemplates<K, V> {

        boolean matches(Item item);

        Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors();

        // Only for testing purposes
        Set<Schema> getExtractorSchemasByTopicName(String topic);

        Set<String> topics();

        default boolean isRegexEnabled() {
            return false;
        }

        Optional<Pattern> subscriptionPattern();
    }

    private static class DefaultSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final String normalizedString;
        private final Schema schema;
        private boolean snapshotFlag;

        DefaultSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
            this.normalizedString = expression.asCanonicalItemName();
            this.schema = expression.schema();
            this.itemHandle = itemHandle;
            this.snapshotFlag = true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, normalizedString);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DefaultSubscribedItem other
                    && normalizedString.equals(other.normalizedString)
                    && Objects.equals(itemHandle, other.itemHandle);
        }

        @Override
        public Schema schema() {
            return schema;
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
        public String asCanonicalItemName() {
            return normalizedString;
        }
    }

    private static class ItemTemplate<K, V> {

        private final Schema schema;
        private final String topic;
        private final CanonicalItemExtractor<K, V> extractor;

        ItemTemplate(String topic, CanonicalItemExtractor<K, V> extractor) {
            this.topic = Objects.requireNonNull(topic);
            this.extractor = Objects.requireNonNull(extractor);
            this.schema = extractor.schema();
        }

        public boolean matches(Item item) {
            return schema.equals(item.schema());
        }

        CanonicalItemExtractor<K, V> extractor() {
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
        public Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors() {
            return templates.stream()
                    .collect(
                            groupingBy(
                                    ItemTemplate::topic,
                                    mapping(ItemTemplate::extractor, toSet())));
        }

        @Override
        public Set<String> topics() {
            return templates.stream().map(ItemTemplate::topic).collect(toSet());
        }

        @Override
        public Set<Schema> getExtractorSchemasByTopicName(String topic) {
            return groupExtractors().getOrDefault(topic, emptySet()).stream()
                    .map(CanonicalItemExtractor::schema)
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
        return subscribedFrom(Expressions.Subscription(input), itemHandle);
    }

    static SubscribedItem subscribedFrom(SubscriptionExpression expression, Object itemHandle) {
        return new DefaultSubscribedItem(expression, itemHandle);
    }

    public static <K, V> ItemTemplates<K, V> templatesFrom(
            TopicConfigurations topicsConfig, KeyValueSelectorSuppliers<K, V> sSuppliers)
            throws ExtractionException {
        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicConfiguration topicConfig : topicsConfig.configurations()) {
            for (TemplateExpression template : topicConfig.itemReferences()) {
                templates.add(
                        new ItemTemplate<>(
                                topicConfig.topic(), canonicalItemExtractor(sSuppliers, template)));
            }
        }
        return new DefaultItemTemplates<>(templates, topicsConfig.isRegexEnabled());
    }

    private Items() {}
}
