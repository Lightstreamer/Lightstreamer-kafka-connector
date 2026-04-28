
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

    /** Represents a named item with an associated handle for server-side identification. */
    public interface Item {

        /**
         * Returns the name of this item.
         *
         * @return the item name
         */
        String name();

        /**
         * Returns the handle object used by the Lightstreamer Server to identify this item.
         *
         * @return the item handle
         */
        Object itemHandle();
    }

    /**
     * Represents a subscribed item that can receive snapshot and real-time events from Kafka
     * records routed through the mapping pipeline.
     *
     * <p>Implementations control the event delivery lifecycle: buffering real-time events until
     * snapshot delivery completes, then switching to direct delivery via {@link
     * #enableRealtimeEvents(EventListener)}.
     *
     * @see SubscribedItems
     */
    public interface SubscribedItem extends Item {

        /**
         * Returns the schema associated with this item, used for template matching.
         *
         * @return the {@link Schema} of this item
         */
        Schema schema();

        /**
         * Indicates whether this item is still in the snapshot delivery phase.
         *
         * @return {@code true} if snapshot delivery is in progress, {@code false} otherwise
         */
        boolean isSnapshot();

        /**
         * Sets the snapshot delivery state of this item.
         *
         * @param flag {@code true} to indicate snapshot is in progress, {@code false} when complete
         */
        void setSnapshot(boolean flag);

        /**
         * Returns the canonical item name used for routing and identification.
         *
         * @return the canonical item name
         */
        String asCanonicalItemName();

        /**
         * Enables direct real-time event delivery, draining any buffered events first.
         *
         * @param listener the {@link EventListener} to deliver events to
         */
        void enableRealtimeEvents(EventListener listener);

        /**
         * Sends a real-time event for this item.
         *
         * <p>The default implementation delegates directly to {@link EventListener#update}.
         * Subclasses may override to buffer events during the snapshot phase.
         *
         * @param event the field name-value pairs to send
         * @param listener the {@link EventListener} to deliver the event to
         */
        default void sendRealtimeEvent(Map<String, String> event, EventListener listener) {
            listener.update(this, event, false);
        }

        /**
         * Sends a snapshot event for this item.
         *
         * @param event the field name-value pairs to send
         * @param listener the {@link EventListener} to deliver the event to
         */
        default void sendSnapshotEvent(Map<String, String> event, EventListener listener) {
            listener.update(this, event, true);
        }

        /**
         * Clears the snapshot for this item on the server.
         *
         * @param listener the {@link EventListener} to notify
         */
        default void clearSnapshot(EventListener listener) {
            listener.clearSnapshot(this);
        }

        /**
         * Signals the end of snapshot delivery for this item.
         *
         * @param listener the {@link EventListener} to notify
         */
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
         * Creates an implicit instance of {@code SubscribedItems} that auto-registers items on
         * first access via {@link #getItem(String)}.
         *
         * <p>Items created by this implementation use a lightweight {@link DirectSubscribedItem}
         * that delegates directly to the {@link EventListener} without buffering, making it
         * suitable for the implicit item snapshot strategy where no explicit subscribe/unsubscribe
         * lifecycle is managed.
         *
         * @return a new {@code SubscribedItems} instance that creates items on demand
         */
        static SubscribedItems implicit() {
            return new ImplicitSubscribedItems();
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

    /** No-operation implementation of {@link SubscribedItems} that ignores all mutations. */
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

    /**
     * Default thread-safe implementation of {@link SubscribedItems} backed by a {@link
     * ConcurrentHashMap}.
     */
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

    /**
     * A {@link SubscribedItems} implementation that auto-registers items on first access. When
     * {@link #getItem(String)} is called with a canonical name not yet in the map, a lightweight
     * {@link DirectSubscribedItem} is created and cached atomically, using the canonical name as
     * the item handle.
     *
     * <p>Designed for the implicit item snapshot strategy where no explicit subscribe/unsubscribe
     * lifecycle is managed by the adapter.
     */
    private static class ImplicitSubscribedItems implements SubscribedItems {

        private final Map<String, SubscribedItem> items = new ConcurrentHashMap<>();

        @Override
        public void addItem(SubscribedItem item) {
            items.put(item.asCanonicalItemName(), item);
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            return Optional.ofNullable(items.remove(itemName));
        }

        /**
         * {@inheritDoc}
         *
         * <p>If no item exists for the given name, a lightweight {@link DirectSubscribedItem} is
         * created atomically and cached. The item uses the canonical name itself as its handle (no
         * server-side subscription exists in implicit mode) and {@link Schema#nop()} as its schema.
         *
         * <p>Using {@code Schema.nop()} is safe here because implicit items bypass the {@link
         * ItemTemplates#matches(SubscribedItem)} check entirely — routing is performed by canonical
         * name string lookup in this map, not by schema matching. This avoids parsing the
         * subscription expression and allocating a per-item {@code Schema} object, which would
         * otherwise duplicate identical schema data across potentially hundreds of thousands of
         * items (one per unique key in the compacted topic).
         */
        @Override
        public SubscribedItem getItem(String itemName) {
            return items.computeIfAbsent(
                    itemName, name -> new DirectSubscribedItem(name, name, Schema.nop()));
        }

        @Override
        public boolean isEmpty() {
            return items.isEmpty();
        }

        @Override
        public int size() {
            return items.size();
        }

        @Override
        public void clear() {
            items.clear();
        }
    }

    /**
     * A lightweight {@link SubscribedItem} implementation that delivers events directly via the
     * interface default methods, without buffering.
     *
     * <p>Unlike {@link BufferedSubscribedItem}, this implementation allocates no queue, lock, or
     * consumer lambda — all event delivery goes straight to the {@link EventListener}.
     *
     * <p>Used in two scenarios:
     *
     * <ul>
     *   <li>No-snapshot mode with an explicit server-provided item handle
     *   <li>Implicit item snapshot strategy where the canonical name serves as the item handle
     * </ul>
     */
    private static class DirectSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final String canonicalItemName;
        private final Schema schema;

        DirectSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
            this(expression.asCanonicalItemName(), itemHandle, expression.schema());
        }

        DirectSubscribedItem(String canonicalItemName, Object itemHandle, Schema schema) {
            this.canonicalItemName = canonicalItemName;
            this.itemHandle = itemHandle;
            this.schema = schema;
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, canonicalItemName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DirectSubscribedItem other
                    && canonicalItemName.equals(other.canonicalItemName)
                    && Objects.equals(itemHandle, other.itemHandle);
        }

        @Override
        public String name() {
            return canonicalItemName;
        }

        @Override
        public Object itemHandle() {
            return itemHandle;
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public String asCanonicalItemName() {
            return canonicalItemName;
        }

        @Override
        public boolean isSnapshot() {
            return false;
        }

        @Override
        public void setSnapshot(boolean flag) {
            // No-op: direct items do not manage snapshot state
        }

        @Override
        public void enableRealtimeEvents(EventListener listener) {
            // No-op: direct items deliver events immediately via interface defaults
        }
    }

    /**
     * Manages item templates that map Kafka topic records to Lightstreamer items through canonical
     * extraction.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    public interface ItemTemplates<K, V> {

        /**
         * Checks whether any configured template matches the given item's schema.
         *
         * @param item the {@link SubscribedItem} to test
         * @return {@code true} if at least one template matches, {@code false} otherwise
         */
        boolean matches(SubscribedItem item);

        /**
         * Returns extractors grouped by topic name.
         *
         * @return a map from topic name to the set of {@link CanonicalItemExtractor}s for that
         *     topic
         */
        Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors();

        /**
         * Returns the set of extractor schemas configured for the given topic. Intended for testing
         * purposes only.
         *
         * @param topic the Kafka topic name
         * @return the set of {@link Schema}s for that topic
         */
        Set<Schema> getExtractorSchemasByTopicName(String topic);

        /**
         * Returns all topic names covered by the configured templates.
         *
         * @return the set of topic names
         */
        Set<String> topics();

        /**
         * Returns the set of topics that have at least one template matching the given item's
         * schema.
         *
         * @param item the {@link SubscribedItem} to match against configured templates
         * @return the set of topic names whose templates match the item
         */
        Set<String> topicsFor(SubscribedItem item);

        /**
         * Indicates whether regex-based topic matching is enabled.
         *
         * @return {@code true} if regex matching is enabled, {@code false} otherwise
         */
        default boolean isRegexEnabled() {
            return false;
        }

        /**
         * Returns the compiled subscription pattern when regex topic matching is enabled.
         *
         * @return an {@link Optional} containing the compiled {@link Pattern}, or empty if regex is
         *     disabled
         */
        Optional<Pattern> subscriptionPattern();
    }

    /**
     * The default {@link SubscribedItem} implementation that buffers real-time events during the
     * snapshot phase and drains them upon {@link #enableRealtimeEvents(EventListener)}.
     */
    private static class BufferedSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final String canonicalItemName;
        private final Schema schema;
        private volatile Queue<Map<String, String>> pendingRealtimeEvents;
        private volatile BiConsumer<Map<String, String>, EventListener> realtimeEventConsumer;
        private boolean snapshotFlag;

        BufferedSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
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
            return obj instanceof BufferedSubscribedItem other
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

    /**
     * Associates a topic with a {@link CanonicalItemExtractor} and the resulting {@link Schema} for
     * template matching.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
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

    /**
     * Default implementation of {@link ItemTemplates} backed by an immutable list of {@link
     * ItemTemplate} entries.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
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
        public Set<String> topicsFor(SubscribedItem item) {
            return templates.stream()
                    .filter(t -> t.matches(item))
                    .map(ItemTemplate::topic)
                    .collect(toSet());
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

    /**
     * Creates a buffered {@link SubscribedItem} using the input as both the subscription expression
     * source and the item handle.
     *
     * @param input the subscription expression string, also used as the item handle
     * @return a new {@link SubscribedItem} that buffers real-time events until snapshot completes
     * @throws ExpressionException if the input cannot be parsed as a valid subscription expression
     */
    public static SubscribedItem subscribedFrom(String input) throws ExpressionException {
        return subscribedFrom(input, input);
    }

    /**
     * Creates a buffered {@link SubscribedItem} from the given input and item handle.
     *
     * @param input the subscription expression string
     * @param itemHandle the handle object used by the Lightstreamer Server to identify the item
     * @return a new {@link SubscribedItem} that buffers real-time events until snapshot completes
     * @throws ExpressionException if the input cannot be parsed as a valid subscription expression
     */
    public static SubscribedItem subscribedFrom(String input, Object itemHandle)
            throws ExpressionException {
        return subscribedFrom(Expressions.Subscription(input), itemHandle);
    }

    /**
     * Creates a {@link BufferedSubscribedItem} from a pre-parsed subscription expression and item
     * handle.
     *
     * @param expression the parsed {@link SubscriptionExpression}
     * @param itemHandle the handle object used by the Lightstreamer Server to identify the item
     * @return a new {@link SubscribedItem} wrapping the given expression
     */
    static SubscribedItem subscribedFrom(SubscriptionExpression expression, Object itemHandle) {
        return new BufferedSubscribedItem(expression, itemHandle);
    }

    /**
     * Creates an {@link ItemTemplates} instance from the given topic configurations and selector
     * suppliers.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param topicsConfig the {@link TopicConfigurations} defining topic-to-item mappings
     * @param sSuppliers the {@link KeyValueSelectorSuppliers} used to create extractors
     * @return a new {@link ItemTemplates} instance covering all configured topic mappings
     * @throws ExtractionException if an extractor cannot be created from the configuration
     */
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
