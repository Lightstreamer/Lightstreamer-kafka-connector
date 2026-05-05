
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
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Factory and container types for managing subscribed items and item templates in the Lightstreamer
 * Kafka Connector mapping pipeline.
 *
 * <p>This class provides:
 *
 * <ul>
 *   <li>{@link SubscribedItem} — the item abstraction for event delivery
 *   <li>{@link SubscribedItems} — thread-safe collections of subscribed items
 *   <li>{@link ItemTemplates} — topic-to-item mapping via canonical extraction
 *   <li>Factory methods ({@code subscribedFrom}, {@code templatesFrom}) for creating instances
 * </ul>
 */
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
     * <p>Implementations control the event delivery lifecycle: events of either kind (snapshot or
     * real-time) may be buffered until delivery is unlocked via {@link
     * #enableEventsDelivery(EventListener)}, after which both kinds flow directly to the {@link
     * EventListener}.
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
         * Unlocks event delivery for this item: drains any events (snapshot or real-time) that were
         * buffered before this call, preserving insertion order and the original {@code isSnapshot}
         * flag of each event, and switches the item to direct delivery for all subsequent events.
         *
         * @param listener the {@link EventListener} to deliver events to
         */
        void enableEventsDelivery(EventListener listener);

        /**
         * Sends a real-time event for this item.
         *
         * <p>The default implementation delegates directly to {@link EventListener#update}.
         * Subclasses may override to buffer events until delivery is unlocked via {@link
         * #enableEventsDelivery(EventListener)}.
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
         * <p>The default implementation delegates directly to {@link EventListener#update}.
         * Subclasses may override to buffer events until delivery is unlocked via {@link
         * #enableEventsDelivery(EventListener)}.
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
         * Creates a new empty instance of {@code SubscribedItems} that requires items to be
         * explicitly added via {@link #addItem(SubscribedItem)} before they can be retrieved.
         *
         * @return a new explicit {@code SubscribedItems} instance
         */
        static SubscribedItems explicit() {
            return new ConcurrentSubscribedItems();
        }

        static ForcedSubscribedItems forced(Supplier<EventListener> listenerSupplier) {
            return new ForcedSubscribedItems(listenerSupplier);
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

        @Override
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
     * {@link SubscribedItems} implementation backing the eager / forced-subscription snapshot
     * strategy. On the first {@link #getItem(String)} call for a canonical name, invokes the
     * configured {@code onForce} callback (typically {@code ItemEventListener.forceSubscription})
     * to instruct the Server to open the subscription synchronously, then caches a stateless {@link
     * DirectSubscribedItem} for the same name. Subsequent calls return the cached item without
     * re-forcing.
     *
     * <p>{@link #releaseAll()} pairs each entry with a call to the configured {@code onUnforce}
     * callback and clears the map; intended for adapter shutdown.
     */
    public static class ForcedSubscribedItems implements SubscribedItems {

        private final Map<String, SubscribedItem> items = new ConcurrentHashMap<>();
        private final Supplier<EventListener> listenerSupplier;

        ForcedSubscribedItems(Supplier<EventListener> listenerSupplier) {
            this.listenerSupplier = Objects.requireNonNull(listenerSupplier, "listenerSupplier");
        }

        @Override
        public void addItem(SubscribedItem item) {
            // No-op: items are created on demand by getItem() via forceSubscription.
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            SubscribedItem removed = items.remove(itemName);
            if (removed != null) {
                listenerSupplier.get().unforceSubscription(itemName);
            }
            return Optional.ofNullable(removed);
        }

        @Override
        public SubscribedItem getItem(String itemName) {
            return items.computeIfAbsent(
                    itemName,
                    name -> {
                        listenerSupplier.get().forceSubscription(name);
                        return new DirectSubscribedItem(name, name, Schema.nop());
                    });
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

        /**
         * Releases every forced item by invoking {@code unforceSubscription} on the resolved {@link
         * EventListener} for each entry, then clears the map. Safe to call on shutdown; idempotent.
         */
        public void releaseAll() {
            EventListener listener = listenerSupplier.get();
            for (String itemName : items.keySet()) {
                listener.unforceSubscription(itemName);
            }
            items.clear();
        }
    }

    /**
     * Thread-safe implementation of {@link SubscribedItems} backed by a {@link ConcurrentHashMap}
     * for explicit (on-demand) mode. Items must be added via {@link #addItem(SubscribedItem)}
     * before they can be retrieved.
     */
    private static class ConcurrentSubscribedItems implements SubscribedItems {

        private final Map<String, SubscribedItem> items = new ConcurrentHashMap<>();

        @Override
        public void addItem(SubscribedItem item) {
            items.put(item.asCanonicalItemName(), item);
        }

        @Override
        public Optional<SubscribedItem> removeItem(String itemName) {
            return Optional.ofNullable(items.remove(itemName));
        }

        @Override
        public SubscribedItem getItem(String itemName) {
            return items.get(itemName);
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
     * <p>Unlike {@code BufferedSubscribedItem}, this implementation allocates no queue, lock, or
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

        DirectSubscribedItem(String canonicalItemName, Object itemHandle, Schema schema) {
            this.itemHandle = itemHandle;
            this.canonicalItemName = canonicalItemName;
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
        public void enableEventsDelivery(EventListener listener) {
            // No-op: direct items deliver events immediately via interface defaults
        }
    }

    /**
     * The default {@link SubscribedItem} implementation that buffers <em>every</em> event (snapshot
     * or real-time) until delivery is unlocked via {@link #enableEventsDelivery(EventListener)}, at
     * which point the buffered events are drained in insertion order — each preserving its original
     * {@code isSnapshot} flag — and the item switches to direct delivery for all subsequent events.
     */
    private static class BufferedSubscribedItem implements SubscribedItem {

        /**
         * Functional interface for dispatching an event with its {@code isSnapshot} flag.
         * Implementations may either enqueue the event or deliver it directly to the listener.
         */
        @FunctionalInterface
        private interface EventDispatcher {
            void dispatch(Map<String, String> event, boolean isSnapshot, EventListener listener);
        }

        /** A buffered event together with its original {@code isSnapshot} flag. */
        private record PendingEvent(Map<String, String> event, boolean isSnapshot) {}

        private final Object itemHandle;
        private final String canonicalItemName;
        private final Schema schema;
        private volatile Queue<PendingEvent> pendingEvents;
        private volatile EventDispatcher dispatcher;
        private boolean snapshotFlag;

        BufferedSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
            this.itemHandle = itemHandle;
            this.canonicalItemName = expression.asCanonicalItemName();
            this.schema = expression.schema();
            this.snapshotFlag = true;
            this.pendingEvents = new ConcurrentLinkedQueue<>();
            this.dispatcher =
                    (event, isSnapshot, listener) -> {
                        // Queue the event for later delivery, preserving its flag.
                        pendingEvents.add(new PendingEvent(event, isSnapshot));
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

        private EventDispatcher directDispatcher() {
            return (event, isSnapshot, listener) -> listener.update(this, event, isSnapshot);
        }

        @Override
        public void enableEventsDelivery(EventListener listener) {
            if (pendingEvents == null) {
                // Already unlocked - no action needed
                return;
            }
            // Create a final dispatcher that flushes queue first, then switches to direct mode.
            // This handles the race where a producer thread observes the buffering dispatcher and
            // enqueues an event after the unlocking thread has already drained the queue.
            ReentrantLock lock = new ReentrantLock();
            EventDispatcher finalDispatcher =
                    (event, isSnapshot, eventListener) -> {
                        // First, drain any remaining events from the queue to maintain ordering.
                        lock.lock();
                        try {
                            if (pendingEvents != null) {
                                drainPendingEvents(eventListener);
                                // Clear the queue reference to help GC (no longer needed).
                                pendingEvents = null;
                            }
                        } finally {
                            lock.unlock();
                        }

                        // Then deliver the current event directly.
                        eventListener.update(this, event, isSnapshot);

                        // After draining, switch to the direct dispatcher for optimal performance.
                        dispatcher = directDispatcher();
                    };

            // Drain any pending events before switching.
            drainPendingEvents(listener);

            // Atomic switch to the self-draining dispatcher.
            dispatcher = finalDispatcher;
        }

        private void drainPendingEvents(EventListener listener) {
            PendingEvent pending;
            // Drain all events from the queue, preserving each event's original flag.
            while ((pending = pendingEvents.poll()) != null) {
                listener.update(this, pending.event(), pending.isSnapshot());
            }
        }

        @Override
        public void sendRealtimeEvent(Map<String, String> event, EventListener listener) {
            // Simple volatile read + call - no synchronization needed!
            dispatcher.dispatch(event, false, listener);
        }

        @Override
        public void sendSnapshotEvent(Map<String, String> event, EventListener listener) {
            // Buffered like real-time events; drained in insertion order on unlock.
            dispatcher.dispatch(event, true, listener);
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
     * Creates a {@code BufferedSubscribedItem} from a pre-parsed subscription expression and item
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
