
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
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.selectors.CanonicalItemExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

public class Items {

    public interface Item {

        String name();

        Object itemHandle();
    }

    public interface SubscribedItem extends Item {

        Schema schema();

        boolean isSnapshot();

        void setSnapshot(boolean flag);

        String asCanonicalItemName();

        void enableRealtimeEvents(EventListener listener);

        default void sendRealtimeEvent(Map<String, String> event, EventListener listener) {
            listener.update(this, event, false);
        }

        default void sendSnapshotEvent(Map<String, String> event, EventListener listener) {
            listener.update(this, event, true);
        }

        default void clearSnapshot(EventListener listener) {
            listener.clearSnapshot(this);
        }

        default void endOfSnapshot(EventListener listener) {
            listener.endOfSnapshot(this);
        }
    }

    /**
     * Interface for managing a collection of subscribed items in the Lightstreamer Kafka connector.
     *
     * <p>This interface provides operations to manage subscriptions including adding, removing, and
     * retrieving subscribed items. It also provides utility methods to check the state of the
     * subscription collection and factory methods to create instances.
     *
     * <p>Implementations of this interface should handle the lifecycle of subscribed items and
     * provide thread-safe operations when used in concurrent environments.
     *
     * <p>The interface supports both active implementations that manage actual subscriptions and
     * no-operation implementations for scenarios where subscription management is disabled.
     *
     * @see SubscribedItem
     */
    public interface SubscribedItems {

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
         * @return the {@link SubscribedItem} associated with the given name, or {@code null} if no
         *     item with the specified name is found
         */
        SubscribedItem getItem(String itemName);

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
        public void addItem(SubscribedItem item) {
            // No operation
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            return Optional.empty();
        }

        public SubscribedItem getItem(String itemName) {
            return null;
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
        public SubscribedItem getItem(String itemName) {
            return sourceItems.get(itemName);
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

        boolean matches(SubscribedItem item);

        Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors();

        // Only for testing purposes
        Set<Schema> getExtractorSchemasByTopicName(String topic);

        Set<String> topics();

        default boolean isRegexEnabled() {
            return false;
        }

        Optional<Pattern> subscriptionPattern();
    }

    private static class ImplicitItem implements SubscribedItem {

        private final String name;

        private boolean snapshotFlag = true;

        private ImplicitItem(String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Object itemHandle() {
            return name;
        }

        @Override
        public Schema schema() {
            return Schema.nop();
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
            return name;
        }

        @Override
        public void enableRealtimeEvents(EventListener listener) {
            // No operation
        }
    }

    private static class DefaultSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final String canonicalItemName;
        private final Schema schema;
        private boolean snapshotFlag;

        private volatile Queue<Map<String, String>> pendingRealtimeEvents;

        private volatile BiConsumer<Map<String, String>, EventListener> realtimeEventConsumer;

        DefaultSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
            this.canonicalItemName = expression.asCanonicalItemName();
            this.schema = expression.schema();
            this.itemHandle = itemHandle;
            this.snapshotFlag = true;
            this.pendingRealtimeEvents = new ConcurrentLinkedQueue<>();
            this.realtimeEventConsumer =
                    (event, listener) -> {
                        // Queue the event for later processing
                        pendingRealtimeEvents.add(event);
                    };
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, canonicalItemName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DefaultSubscribedItem other
                    && canonicalItemName.equals(other.canonicalItemName)
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
            return canonicalItemName;
        }

        @Override
        public String name() {
            return canonicalItemName;
        }

        private BiConsumer<Map<String, String>, EventListener> directConsumer() {
            return (event, listener) -> listener.update(this, event, false);
        }

        @Override
        public void enableRealtimeEvents(EventListener listener) {
            if (pendingRealtimeEvents == null) {
                // Already enabled - no action needed
                return;
            }
            // Create a final consumer that flushes queue first, then switches to direct mode
            ReentrantLock lock = new ReentrantLock();
            BiConsumer<Map<String, String>, EventListener> finalConsumer =
                    (event, eventListener) -> {
                        // First, drain any remaining events from queue to maintain ordering
                        lock.lock();
                        try {
                            if (pendingRealtimeEvents != null) {
                                drainPendingRealtimeEvents(eventListener);
                                // Clear the queue to help GC (no longer needed)
                                pendingRealtimeEvents = null;
                            }
                        } finally {
                            lock.unlock();
                        }

                        // Then process the current event
                        eventListener.update(this, event, false);

                        // After draining queue, switch to direct consumer for optimal performance
                        realtimeEventConsumer = directConsumer();
                    };

            // Drain any pending events before switching
            drainPendingRealtimeEvents(listener);

            // Atomic switch to self-draining consumer
            realtimeEventConsumer = finalConsumer;
        }

        private void drainPendingRealtimeEvents(EventListener listener) {
            Map<String, String> event;
            // Drain all events from queue
            while ((event = pendingRealtimeEvents.poll()) != null) {
                listener.update(this, event, false);
            }
        }

        @Override
        public void sendRealtimeEvent(Map<String, String> event, EventListener listener) {
            // Simple volatile read + call - no synchronization needed!
            realtimeEventConsumer.accept(event, listener);
        }

        @Override
        public void sendSnapshotEvent(Map<String, String> event, EventListener listener) {
            listener.update(this, event, true);
        }

        @Override
        public void clearSnapshot(EventListener listener) {
            listener.clearSnapshot(this);
        }

        @Override
        public void endOfSnapshot(EventListener listener) {
            listener.endOfSnapshot(this);
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

        public boolean matches(SubscribedItem item) {
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
        public boolean matches(SubscribedItem item) {
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

    public static SubscribedItem implicitFrom(String name) {
        return new ImplicitItem(name);
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
