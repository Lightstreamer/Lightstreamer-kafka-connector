
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.metadata.Mode;
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

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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
 *   <li>Factory methods ({@code onDemandSubscribedFrom}, {@code forceableSubscribedFrom}, {@code
 *       templatesFrom}) for creating instances
 * </ul>
 */
public class Items {

    /** Represents a named item with an associated handle for server-side identification. */
    public interface Item {

        /**
         * Returns the canonical name of this item.
         *
         * @return the canonical item name
         */
        String canonicalName();
    }

    /**
     * Represents a subscribed item that can receive snapshot and real-time events from Kafka
     * records routed through the mapping pipeline.
     *
     * <p>Implementations control the event delivery lifecycle: events may be delivered immediately
     * to an {@link ItemEventListener}, or held until delivery is explicitly enabled by the
     * collection that owns the item.
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
         * Delivers a real-time (non-snapshot) event for this item to the given listener.
         *
         * @param event the field map to deliver
         * @param listener the {@link ItemEventListener} to dispatch the event through
         */
        void sendRealTimeEvent(Map<String, String> event, ItemEventListener listener);

        /**
         * Delivers a snapshot event for this item to the given listener. The default implementation
         * forwards to {@link #sendRealTimeEvent(Map, ItemEventListener)}.
         *
         * @param event the field map to deliver
         * @param listener the {@link ItemEventListener} to dispatch the event through
         */
        default void sendSnapshotEvent(Map<String, String> event, ItemEventListener listener) {
            sendRealTimeEvent(event, listener);
        }
    }

    /**
     * Default {@link SubscribedItem} implementation, used by the on-demand pipeline. The {@code
     * itemHandle} is bound at construction and never changes.
     *
     * <p>Equality is by canonical item name.
     */
    public static class OnDemandSubscribedItem implements SubscribedItem {

        private final String canonicalItemName;
        private final Schema schema;
        private final Object itemHandle;

        OnDemandSubscribedItem(SubscriptionExpression expression, Object itemHandle) {
            this.canonicalItemName = expression.canonicalItemName();
            this.schema = expression.schema();
            this.itemHandle = Objects.requireNonNull(itemHandle, "itemHandle");
        }

        @Override
        public int hashCode() {
            return Objects.hash(canonicalItemName, itemHandle);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof OnDemandSubscribedItem other
                    && canonicalItemName.equals(other.canonicalItemName)
                    && itemHandle.equals(other.itemHandle);
        }

        @Override
        public String canonicalName() {
            return canonicalItemName;
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public void sendRealTimeEvent(Map<String, String> event, ItemEventListener listener) {
            listener.smartUpdate(itemHandle, event, false);
        }
    }

    /**
     * Entry type stored by {@link ForceableSubscribedItems} to back the forceable /
     * forced-subscription snapshot strategy. Carries the {@code forced} flag (eternal-state marker
     * promoted via {@code forceSubscription}) and the {@code lastAccessNanos} stamp consulted by
     * {@link ForceableSubscribedItems#clearIdleSnapshots(long)}.
     *
     * <p>Operates in two successive modes:
     *
     * <ul>
     *   <li><strong>Queueing mode</strong> (initial state): buffers <em>every</em> event (snapshot
     *       or real-time, including {@code clearSnapshot} and {@code endOfSnapshot} signals) into
     *       an internal queue, because no Server-allocated handle has been bound yet.
     *   <li><strong>Direct-dispatch mode</strong>: activated by the first call to {@link
     *       #enableEventsDelivery(Object, ItemEventListener)}, which drains the queued events in
     *       insertion order — each preserving its original {@code isSnapshot} flag — and then
     *       forwards all subsequent events straight to the bound handle.
     * </ul>
     */
    public static class ForceableSubscribedItem implements SubscribedItem {

        /**
         * Functional interface for dispatching an event with its {@code isSnapshot} flag.
         * Implementations may either enqueue the event or deliver it directly to the listener.
         */
        private interface EventDispatcher {
            void dispatchUpdate(
                    Map<String, String> event, boolean isSnapshot, ItemEventListener listener);

            void clearSnapshot(ItemEventListener listener);

            void endOfSnapshot(ItemEventListener listener);
        }

        /**
         * {@link EventDispatcher} that delivers events directly to the {@link ItemEventListener},
         * keyed by the bound {@code itemHandle}. Used after the item has been activated and is no
         * longer buffering.
         */
        private static final class DirectEventDispatcher implements EventDispatcher {
            private final Object itemHandle;

            private DirectEventDispatcher(Object itemHandle) {
                this.itemHandle = itemHandle;
            }

            @Override
            public void dispatchUpdate(
                    Map<String, String> event, boolean isSnapshot, ItemEventListener listener) {
                listener.smartUpdate(itemHandle, event, isSnapshot);
            }

            @Override
            public void clearSnapshot(ItemEventListener listener) {
                listener.smartClearSnapshot(itemHandle);
            }

            @Override
            public void endOfSnapshot(ItemEventListener listener) {
                listener.smartEndOfSnapshot(itemHandle);
            }
        }

        /**
         * {@link EventDispatcher} that buffers every event into an internal {@link Queue} of {@link
         * PendingEvent}s. Used while the item is in queueing mode, before a handle has been bound
         * via {@link #enableEventsDelivery(Object, ItemEventListener)}. The queued events are
         * drained in insertion order via {@link #drainTo(Object, ItemEventListener)} when the item
         * is activated.
         *
         * <p>Concurrency: every dispatch method is {@code synchronized} on the dispatcher instance,
         * and activation also synchronizes on the same instance to drain and swap the owner's
         * dispatcher field atomically. A producer that already entered the synchronized region but
         * raced behind activation re-reads the owner's dispatcher and routes the event to the
         * post-activation {@link DirectEventDispatcher} instead of enqueueing into a queue that
         * nobody will ever drain.
         */
        private static final class QueueingEventDispatcher implements EventDispatcher {

            private final ForceableSubscribedItem owner;
            private final Queue<PendingEvent> pendingEvents = new ArrayDeque<>();

            QueueingEventDispatcher(ForceableSubscribedItem owner) {
                this.owner = owner;
            }

            @Override
            public synchronized void dispatchUpdate(
                    Map<String, String> event, boolean isSnapshot, ItemEventListener listener) {
                EventDispatcher current = owner.dispatcher;
                if (current != this) {
                    current.dispatchUpdate(event, isSnapshot, listener);
                    return;
                }
                pendingEvents.add(PendingEvent.update(event, isSnapshot));
            }

            @Override
            public synchronized void clearSnapshot(ItemEventListener listener) {
                EventDispatcher current = owner.dispatcher;
                if (current != this) {
                    current.clearSnapshot(listener);
                    return;
                }
                pendingEvents.add(PendingEvent.clearSnapshot());
            }

            @Override
            public synchronized void endOfSnapshot(ItemEventListener listener) {
                EventDispatcher current = owner.dispatcher;
                if (current != this) {
                    current.endOfSnapshot(listener);
                    return;
                }
                pendingEvents.add(PendingEvent.endOfSnapshot());
            }

            /**
             * Polls every queued event and dispatches it to {@code listener} keyed by {@code
             * handle}, preserving each event's original {@code isSnapshot} flag. The caller must
             * hold {@code synchronized (this)}.
             */
            void drainTo(Object handle, ItemEventListener listener) {
                assert Thread.holdsLock(this);
                PendingEvent pending;
                while ((pending = pendingEvents.poll()) != null) {
                    switch (pending.type()) {
                        case UPDATE ->
                                listener.smartUpdate(handle, pending.event(), pending.isSnapshot());
                        case CLEAR_SNAPSHOT -> listener.smartClearSnapshot(handle);
                        case END_OF_SNAPSHOT -> listener.smartEndOfSnapshot(handle);
                    }
                }
            }
        }

        /**
         * A buffered event, tagged by {@link EventType} and carrying its {@code isSnapshot} flag
         * for {@link EventType#UPDATE} entries.
         */
        private record PendingEvent(EventType type, Map<String, String> event, boolean isSnapshot) {

            enum EventType {
                UPDATE,
                CLEAR_SNAPSHOT,
                END_OF_SNAPSHOT
            }

            static PendingEvent update(Map<String, String> event, boolean isSnapshot) {
                return new PendingEvent(EventType.UPDATE, event, isSnapshot);
            }

            static PendingEvent clearSnapshot() {
                return new PendingEvent(EventType.CLEAR_SNAPSHOT, null, false);
            }

            static PendingEvent endOfSnapshot() {
                return new PendingEvent(EventType.END_OF_SNAPSHOT, null, false);
            }
        }

        // VarHandle on lastAccessNanos: lets clearIdleSnapshots atomically claim an
        // expiration via compareAndSet, rejecting the dispatch if a concurrent touch()
        // bumped the stamp between the idle check and the claim.
        private static final VarHandle LAST_ACCESS_NANOS;

        static {
            try {
                LAST_ACCESS_NANOS =
                        MethodHandles.lookup()
                                .findVarHandle(
                                        ForceableSubscribedItem.class,
                                        "lastAccessNanos",
                                        long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        /**
         * Current event dispatcher. Starts as a {@link QueueingEventDispatcher} that buffers every
         * event (queueing mode), and is atomically swapped to a {@link DirectEventDispatcher} the
         * first time {@link #enableEventsDelivery(Object, ItemEventListener)} runs (direct-dispatch
         * mode). Declared {@code volatile} because the swap must be observed by producer threads
         * dispatching on the event path.
         */
        protected volatile EventDispatcher dispatcher;

        private final String canonicalItemName;
        private final Schema schema;
        private volatile boolean forced;
        private volatile long lastAccessNanos;
        private QueueingEventDispatcher queueingDispatcher;

        ForceableSubscribedItem(SubscriptionExpression expression) {
            this.canonicalItemName = expression.canonicalItemName();
            this.schema = expression.schema();
            this.queueingDispatcher = new QueueingEventDispatcher(this);
            this.dispatcher = queueingDispatcher;
            this.lastAccessNanos = System.nanoTime();
        }

        @Override
        public String canonicalName() {
            return canonicalItemName;
        }

        @Override
        public Schema schema() {
            return schema;
        }

        /**
         * Activates this item for direct event delivery. Drains any events buffered while in
         * queueing mode to the given {@link ItemEventListener} keyed by {@code itemHandle}, then
         * switches to direct-dispatch mode for all subsequent events. If already activated, this
         * method is a no-op.
         *
         * @param itemHandle the handle allocated by the Lightstreamer Server for this item
         * @param listener the {@link ItemEventListener} to deliver drained and future events to
         */
        public void enableEventsDelivery(Object itemHandle, ItemEventListener listener) {
            Objects.requireNonNull(itemHandle, "itemHandle");
            QueueingEventDispatcher q = queueingDispatcher;
            if (q == null) {
                // Already activated — nothing to do.
                return;
            }
            // Drain and swap atomically under the queueing dispatcher's monitor. Any producer
            // contending on the same monitor either finishes its enqueue before us (we drain it)
            // or enters after the swap and re-routes to DirectEventDispatcher via the redirect
            // check in QueueingEventDispatcher's synchronized methods.
            synchronized (q) {
                q.drainTo(itemHandle, listener);
                dispatcher = new DirectEventDispatcher(itemHandle);
            }
            // Drop the field reference. The QueueingEventDispatcher (with its empty queue) is now
            // unreachable: producers that observed the swap route through DirectEventDispatcher
            // and never look at this field again. At 1M items this releases ~150 MB of state.
            queueingDispatcher = null;
        }

        @Override
        public void sendRealTimeEvent(Map<String, String> event, ItemEventListener listener) {
            dispatcher.dispatchUpdate(event, false, listener);
        }

        @Override
        public void sendSnapshotEvent(Map<String, String> event, ItemEventListener listener) {
            dispatcher.dispatchUpdate(event, true, listener);
        }

        /**
         * Dispatches a {@code clearSnapshot} signal for this item. In queueing mode the signal is
         * buffered; in direct-dispatch mode it is delivered immediately via {@code
         * smartClearSnapshot} on the bound handle.
         *
         * @param listener the {@link ItemEventListener} to dispatch the signal through
         */
        public void clearSnapshot(ItemEventListener listener) {
            dispatcher.clearSnapshot(listener);
        }

        /**
         * Dispatches an {@code endOfSnapshot} signal for this item. In queueing mode the signal is
         * buffered; in direct-dispatch mode it is delivered immediately via {@code
         * smartEndOfSnapshot} on the bound handle.
         *
         * @param listener the {@link ItemEventListener} to dispatch the signal through
         */
        public void endOfSnapshot(ItemEventListener listener) {
            dispatcher.endOfSnapshot(listener);
        }

        /**
         * Marks this item as forced (i.e., promoted via {@code forceSubscription}). Used by {@link
         * ForceableSubscribedItems} as the eternal-state marker; writers hold the per-name lock and
         * the volatile write publishes the bit to the lock-free fast-path reader. The flag is
         * monotonic.
         */
        void markForced() {
            this.forced = true;
        }

        /**
         * Refreshes the last-access stamp used by the idle-expiration scheduler. Uses {@link
         * System#nanoTime()} so the elapsed measurement is monotonic and immune to wall-clock jumps
         * (NTP, leap second, host suspend/resume).
         */
        void touch() {
            this.lastAccessNanos = System.nanoTime();
        }

        /**
         * Returns whether this item has been forced. Safe to call without a lock: the field is
         * {@code volatile} and monotonic ({@code true} is terminal).
         */
        boolean isForced() {
            return forced;
        }

        /**
         * Returns the {@link System#nanoTime()} stamp of the last {@link #touch()} (or of
         * construction, if never touched). Read by {@link
         * ForceableSubscribedItems#clearIdleSnapshots(long)} to compute idle time; paired with
         * {@link #casLastTouched(long, long)} to claim an expiration atomically.
         *
         * @return the last-access {@code nanoTime} stamp
         */
        long lastTouched() {
            return lastAccessNanos;
        }

        /**
         * Atomically updates {@code lastAccessNanos} from {@code expected} to {@code update}. Used
         * by {@link ForceableSubscribedItems#clearIdleSnapshots(long)} to claim an expiration
         * decision: if a concurrent {@link #touch()} bumped the stamp after the idle check, this
         * CAS fails and the scheduler skips the item, preventing a stale {@code clearSnapshot} from
         * racing past a fresh update on the SDK delivery queue.
         *
         * @param expected the value previously read via {@link #lastTouched()}
         * @param update the new value to install (typically the scan's reference timestamp)
         * @return {@code true} if the CAS succeeded; {@code false} if a concurrent write bumped the
         *     stamp first
         */
        boolean casLastTouched(long expected, long update) {
            return LAST_ACCESS_NANOS.compareAndSet(this, expected, update);
        }

        // Visible for tests: lets unit tests stamp an arbitrary last-access value to
        // deterministically simulate aged items in clearIdleSnapshots scans, without
        // resorting to Thread.sleep.
        void setLastTouched(long nanos) {
            this.lastAccessNanos = nanos;
        }
    }

    /**
     * Interface for managing a collection of subscribed items in the Lightstreamer Kafka connector.
     *
     * <p>This interface provides operations to retrieve, remove, and iterate over subscribed items.
     * It also provides utility methods to check the state of the subscription collection and
     * factory methods to create instances.
     *
     * <p>Implementations of this interface should handle the lifecycle of subscribed items and
     * provide thread-safe operations when used in concurrent environments.
     *
     * @see SubscribedItem
     */
    public interface SubscribedItems {

        /**
         * Creates a new empty {@link OnDemandSubscribedItems}. Items must be added explicitly via
         * {@link OnDemandSubscribedItems#addItem(OnDemandSubscribedItem)} before they can be
         * retrieved.
         *
         * @return a new {@code OnDemandSubscribedItems} instance
         */
        static OnDemandSubscribedItems onDemand() {
            return new OnDemandSubscribedItems();
        }

        /**
         * Creates a {@code SubscribedItems} backing the forceable / forced-subscription snapshot
         * strategy. On a miss, {@link #getItem(String)} drives {@code
         * ItemEventListener.forceSubscription(name)} so the Server reentrantly binds a handle on
         * the entry; forced entries are eternal for the connection lifetime.
         *
         * @param itemEventListener the {@link ItemEventListener} used to drive {@code
         *     forceSubscription} and to deliver events
         * @param logger the {@link Logger} for recording force-subscription events and errors
         * @return a new {@link ForceableSubscribedItems} instance
         */
        static ForceableSubscribedItems forceable(
                ItemEventListener itemEventListener, Logger logger) {
            return new ForceableSubscribedItems(itemEventListener, logger);
        }

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

        /**
         * Returns the subscribed items as an unmodifiable collection.
         *
         * @return an unmodifiable view of the subscribed items
         */
        Collection<? extends SubscribedItem> values();

        /**
         * Performs the given action for each subscribed item in this collection.
         *
         * @param action the action to perform on each {@link SubscribedItem}
         */
        default void forEach(java.util.function.Consumer<? super SubscribedItem> action) {
            values().forEach(action);
        }
    }

    /**
     * {@link SubscribedItems} implementation backing the forceable / forced-subscription snapshot
     * strategy, selected when {@code item.snapshot.mode = ENABLED}.
     *
     * <p>The map holds a single entry type, {@link ForceableSubscribedItem}, regardless of the path
     * that installed it. A {@code ForceableSubscribedItem} starts in queueing mode (events
     * accumulate in an internal queue) and switches to direct-dispatch mode the first time {@link
     * ForceableSubscribedItem#enableEventsDelivery(Object, ItemEventListener)} is called, which
     * also drains any queued events against the supplied handle. Each entry also carries a
     * monotonic {@code forced} flag (see {@link ForceableSubscribedItem#isForced()}) used as the
     * lock-free fast-path predicate in {@link #getItem(String)}: once set it is never reset, and a
     * forced entry is eternal for the connection lifetime (per the SDK contract: after a successful
     * {@code forceSubscription} no further {@code subscribe}/{@code unsubscribe} callbacks fire for
     * the name).
     *
     * <p>Concurrency model: a per-name {@link ReentrantLock} serializes <em>structural</em>
     * transitions on the entry for a given canonical name (install, activate, prune). The lock is
     * <strong>not</strong> held across the call to {@code
     * ItemEventListener.forceSubscription(name)}: under the SDK contract that call dispatches the
     * Path-2 {@code subscribe(name, handle)} callback on a Server thread (not the calling poll
     * thread) and blocks until it returns; holding the per-name lock across the call would deadlock
     * with the Server thread's own attempt to acquire it from {@link
     * #activateOrInstall(SubscriptionExpression, Object)}.
     *
     * <p>Entries are installed via two convergent paths:
     *
     * <ul>
     *   <li><strong>Path 1 (organic).</strong> The Server calls {@code subscribe(name, handle)}
     *       because a client expressed interest. {@code ForceableSubscriptionsHandler} routes
     *       through {@link #activateOrInstall(SubscriptionExpression, Object)}, which under the
     *       per-name lock installs a fresh {@code ForceableSubscribedItem} and immediately switches
     *       it to direct-dispatch mode bound to the Server-allocated handle, then emits {@code
     *       endOfSnapshot} on the new client subscription. The entry is unforced until the
     *       record-processing thread first observes a record for the name (see {@link
     *       #getItem(String)}).
     *   <li><strong>Path 2 (record-driven).</strong> The record-processing thread calls {@link
     *       #getItem(String)}; on a miss this installs a {@code ForceableSubscribedItem}
     *       placeholder in queueing mode, releases the lock, and calls {@code
     *       forceSubscription(name)}. The Server thread runs {@code subscribe(name, handle)}; the
     *       handler routes through {@link #activateOrInstall(SubscriptionExpression, Object)},
     *       which under the lock detects the placeholder, drains it against the new handle,
     *       switches it to direct-dispatch mode, and marks it forced. {@code endOfSnapshot} is
     *       skipped for this case (the virtual handle has no client to receive it). When {@code
     *       forceSubscription} returns, the record-processing thread re-reads the (now activated
     *       and forced) entry.
     * </ul>
     *
     * <p>Forced entries are eternal. Unforced Path-1 entries are pruned on {@code unsubscribe} via
     * {@link #removeIfUnforced(String)}; Path-2 placeholders are also unforced but cannot coexist
     * with an organic {@code unsubscribe} per the SDK's per-name serialization contract, so this
     * case does not arise in practice.
     */
    public static class ForceableSubscribedItems implements SubscribedItems {

        private final ItemEventListener itemEventListener;
        private final Logger logger;
        private final Map<String, ForceableSubscribedItem> items = new ConcurrentHashMap<>();
        private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();

        ForceableSubscribedItems(ItemEventListener itemEventListener, Logger logger) {
            this.itemEventListener = Objects.requireNonNull(itemEventListener, "itemEventListener");
            this.logger = logger;
        }

        /**
         * Single install-or-activate primitive for the {@code subscribe(name, handle)} entry point.
         * If a {@link ForceableSubscribedItem} placeholder is already present for the canonical
         * name (Path-2 activation in flight, triggered by a record-processing-thread {@code
         * forceSubscription}), drains the placeholder against the new handle, switches it to
         * direct-dispatch mode, marks it forced, and returns {@code null}. Otherwise installs a
         * fresh {@code ForceableSubscribedItem} already in direct-dispatch mode bound to the new
         * handle (Path-1 organic install), emits {@code endOfSnapshot} on it, and returns the
         * freshly installed entry.
         *
         * @param expression the subscription expression for the item
         * @param handle the Server-allocated handle
         * @return the freshly installed entry on Path-1 organic install, or {@code null} on Path-2
         *     activation
         */
        public ForceableSubscribedItem activateOrInstall(
                SubscriptionExpression expression, Object handle) {
            String canonicalName = expression.canonicalItemName();
            ReentrantLock lock = lockFor(canonicalName);
            lock.lock();
            try {
                ForceableSubscribedItem existing = items.get(canonicalName);
                // Two mutually exclusive cases (per SDK contract C-serial, no concurrent
                // subscribe/unsubscribe for the same name can race against this method):
                //
                //   (A) existing != null  --> Path-2 activation.
                //       The entry is a placeholder previously installed by getItem() on the
                //       record-processing thread (lock-held branch "true miss"), currently in
                // queueing
                //       mode with no handle bound. We are running on a Server thread inside
                //       the forceSubscription(name) callback that getItem() triggered.
                //       Action: bind the Server-allocated handle, switch the dispatcher to
                //       direct-delivery (this drains any events queued in the meantime to
                //       the new handle), and mark the entry forced (eternal). After this
                //       method returns the Server unblocks getItem(), which then calls
                //       markForced() again (idempotent, see getItem()). Returns null;
                //       endOfSnapshot is NOT emitted (the virtual handle has no client
                //       to receive it).
                //
                //   (B) existing == null  --> Path-1 organic install.
                //       No record for `name` has been observed yet by a record-processing thread,
                // so
                //       getItem() never installed a placeholder. This is a plain on-demand
                //       subscribe coming from the Server's normal subscription flow.
                //       Action: create a fresh entry already bound to the handle and
                //       already in direct-dispatch mode. It is left UNFORCED on purpose:
                //       the first record-processing thread getItem(name) will promote it via the
                //       hit-on-unforced branch (a forceSubscription no-op per C-fs-noop,
                //       followed by markForced()). Emits endOfSnapshot on the fresh entry
                //       (client subscription needs an end-of-snapshot signal) and returns it.
                if (existing != null) {
                    existing.enableEventsDelivery(handle, itemEventListener);
                    existing.markForced();
                    return null;
                }
                ForceableSubscribedItem fresh = new ForceableSubscribedItem(expression);
                fresh.enableEventsDelivery(handle, itemEventListener);
                items.put(canonicalName, fresh);
                // Path-1 organic: emit end-of-snapshot for the new client subscription.
                // Skipped on Path-2 (the early-return branch above): the virtual handle
                // has no client to receive it, and the seed record is about to be
                // dispatched against it as the first server-pinned snapshot value.
                fresh.endOfSnapshot(itemEventListener);
                return fresh;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public ForceableSubscribedItem getItem(String itemName) {
            // Lock-free fast path. Once an entry is forced it is eternal (per SDK contract
            // C-eternal: no further subscribe/unsubscribe callbacks fire for the name) and
            // its dispatcher is stable in direct-dispatch mode bound to a fixed handle, so
            // subsequent poll-thread sightings can return it without locking.
            ForceableSubscribedItem cached = items.get(itemName);
            if (cached != null && cached.isForced()) {
                // Sliding-touch on the steady-state hot path: every record sighting
                // of an eternal item refreshes the idle clock used by
                // clearIdleSnapshots(). This is what makes the snapshot
                // max-idle policy actually slide.
                cached.touch();
                return cached;
            }

            // Slow path. The per-name lock is held only across structural inspection /
            // mutation; it is RELEASED across forceSubscription, because that call blocks
            // until a Server-thread subscribe(name, handle) callback runs and that callback
            // needs to acquire the same lock from activateOrInstall (holding it across the
            // SDK call would self-deadlock).
            ReentrantLock lock = lockFor(itemName);
            lock.lock();
            try {
                cached = items.get(itemName);
                // Three cases under the lock:
                //
                //   (0) cached != null && cached.isForced()  --> race lost (parallel modes).
                //       In UNORDERED, ORDER_BY_PARTITION, or ORDER_BY_KEY modes, another
                //       worker thread entered the slow path for the same name, installed
                //       the placeholder, drove forceSubscription, and the Server-thread
                //       callback marked the entry forced — all between our fast-path
                //       volatile read and our lock acquisition. The entry is fully
                //       activated and eternal; return it directly.
                //
                //   (1) cached == null  --> true miss (Path-2 start).
                //       No subscribe callback has been processed for this name yet (this
                //       is the first record sighting and there was no organic subscribe
                //       in flight either). Install a placeholder in queueing mode (no
                //       handle bound) so that any further records arriving on other worker
                //       threads before the Server-thread callback runs are buffered.
                //       After we release the lock, forceSubscription(name) will trigger
                //       the Server-thread subscribe callback, which routes through
                //       activateOrInstall() case (A) and binds the handle in place.
                // ItemSnapshotEnabledMode
                //   (2) cached != null && !cached.isForced()  --> hit-on-unforced (Path-1).
                //       The entry was put into the map by a prior organic subscribe
                //       (activateOrInstall() case (B)), or another worker thread installed
                //       a placeholder whose forceSubscription callback has not yet
                //       completed. The entry is not yet marked forced. We must still call
                //       forceSubscription(name) for protocol uniformity; per C-fs-noop the
                //       Server treats a second call for an already-subscribed name as a
                //       no-op and does NOT fire a second subscribe callback. The post-lock
                //       markForced() below promotes the entry (idempotent if the callback
                //       races and marks it first).
                if (cached != null && cached.isForced()) {
                    // Same sliding-touch as the fast path above; we reached the
                    // slow path only because we lost the fast-path/lock race.
                    cached.touch();
                    return cached;
                }
                if (cached == null) {
                    cached = new ForceableSubscribedItem(Subscription(itemName));
                    items.put(itemName, cached);
                }
            } finally {
                lock.unlock();
            }

            // Lock released. Drive the SDK protocol. Releasing the lock here is required
            // for CORRECTNESS (deadlock avoidance), not performance: in case (1),
            // forceSubscription blocks until the Server invokes subscribe(name, handle)
            // on a separate Server thread (per SDK contract C-fs-blocks), and that
            // callback routes into activateOrInstall(), which must acquire this same
            // per-name lock. Holding the lock across forceSubscription would block the
            // Server thread on lock() while the record-processing thread blocks on
            // forceSubscription — a classic deadlock; ReentrantLock does not help because
            // the two participants are different threads.
            //
            // Behavior depends on which lock-held case we came from:
            //
            //   - From case (1): forceSubscription blocks until the Server thread invokes
            //     subscribe(name, handle) -> doSubscribe -> activateOrInstall
            //     case (A), which acquires the per-name lock, binds the handle, switches
            //     dispatch to direct, and marks the entry forced. By the time
            //     forceSubscription returns, the entry is already forced; the markForced()
            //     call below is therefore idempotent.
            //
            //   - From case (2): forceSubscription is a server-side no-op (per C-fs-noop)
            //     and returns without firing any callback. If this thread is the first to
            //     reach this point for the name, the entry is still unforced and the
            //     markForced() call below promotes it. In a parallel race where another
            //     thread's callback has already marked it forced, markForced() is
            //     idempotent.
            //
            // `cached` is stable across this call: activateOrInstall mutates the existing
            // entry in place rather than replacing it, and no concurrent remove can race
            // (per C-serial, no organic unsubscribe can fire while we are in flight here).
            Mode mode = itemEventListener.forceSubscription(itemName);
            if (mode == null) {
                logger.atWarn().log("Failed force subscription for item '{}'", itemName);
            }
            cached.markForced();
            // Anchor the idle clock at the moment this entry becomes eternal.
            // Required for case (2) Path-1 promotion: the entry was created by an
            // earlier organic subscribe and its constructor stamp may be
            // arbitrarily stale (no record had been seen yet), so without this
            // touch the next scheduler tick could push a bogus clearSnapshot on
            // an item that just received its first real record. Redundant but
            // harmless in case (1) Path-2 start (constructor ran ms ago in this
            // same call).
            cached.touch();
            return cached;
        }

        /**
         * Clears snapshots for all forced items that have been idle for longer than the specified
         * maximum idle time. This method is intended to be called periodically by a scheduler to
         * enforce the snapshot idle policy.
         *
         * @param maxIdleSeconds the maximum idle time in seconds before a snapshot is considered
         *     idle and cleared
         */
        public void clearIdleSnapshots(long maxIdleSeconds) {
            logger.atInfo().log("Checking for idle snapshots (maxIdleSeconds={})", maxIdleSeconds);
            // Lock-free scan. We deliberately skip the per-name lock that
            // getItem / activateOrInstall / removeIfUnforced acquire, because:
            //
            //   * Only forced (eternal) items are inspected here; their lifecycle
            //     is terminal (C-eternal), so no concurrent structural mutation
            //     (install / activate / prune) can race against this scan.
            //   * The fast path of getItem is itself lock-free: even if we took
            //     the per-name lock here, we could not serialize against the
            //     poll-thread touch().
            //
            // Race protection: a poll-thread touch() that lands between our
            // idle check and our clearSnapshot dispatch could let a stale clear
            // overwrite a fresh update on the SDK delivery queue. We close this
            // window with a compareAndSet on lastAccessNanos: the CAS only
            // succeeds if the stamp we read at idle-check time is still current
            // when we go to dispatch. The narrow residual window (touch after
            // CAS, before the SDK call) is acceptable: by clearSnapshot's
            // contract, any update arriving after a clearSnapshot simply starts
            // a fresh snapshot.
            long nowNanos = System.nanoTime();
            long maxIdleNanos = TimeUnit.SECONDS.toNanos(maxIdleSeconds);
            for (ForceableSubscribedItem item : items.values()) {
                if (!item.isForced()) {
                    continue;
                }
                long touchedAt = item.lastTouched();
                if (nowNanos - touchedAt < maxIdleNanos) {
                    continue;
                }
                if (!item.casLastTouched(touchedAt, nowNanos)) {
                    // A concurrent touch() raced in after the read above; the
                    // item is no longer idle. Skip without dispatching.
                    continue;
                }
                logger.atInfo().log(
                        "Sending clearSnapshot for expired item '{}'", item.canonicalName());
                item.clearSnapshot(itemEventListener);
            }
            logger.atInfo().log("Idle snapshots check completed");
        }

        /**
         * Removes the entry for {@code itemName} if it has not yet been forced. Used by the
         * forceable pipeline's {@code unsubscribe} to prune Path-1 entries that never received a
         * record (and therefore were never promoted to eternal via {@code forceSubscription}).
         * Forced entries are eternal and not removed.
         *
         * @param itemName the canonical item name
         * @return {@code true} if the entry was removed; {@code false} otherwise
         */
        public boolean removeIfUnforced(String itemName) {
            ReentrantLock lock = lockFor(itemName);
            lock.lock();
            try {
                ForceableSubscribedItem existing = items.get(itemName);
                if (existing == null || existing.isForced()) {
                    return false;
                }
                items.remove(itemName);
                return true;
            } finally {
                lock.unlock();
            }
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
        public Collection<ForceableSubscribedItem> values() {
            return Collections.unmodifiableCollection(items.values());
        }

        private ReentrantLock lockFor(String name) {
            return locks.computeIfAbsent(name, k -> new ReentrantLock());
        }
    }

    /**
     * Thread-safe implementation of {@link SubscribedItems} backed by a {@link ConcurrentHashMap}
     * for the on-demand mode. Items must be added via {@link #addItem(OnDemandSubscribedItem)}
     * before they can be retrieved.
     */
    public static class OnDemandSubscribedItems implements SubscribedItems {

        private final Map<String, OnDemandSubscribedItem> items = new ConcurrentHashMap<>();

        /**
         * Adds the given item to this collection, keyed by its canonical name.
         *
         * @param item the {@link OnDemandSubscribedItem} to add
         */
        public void addItem(OnDemandSubscribedItem item) {
            items.put(item.canonicalName(), item);
        }

        @Override
        public OnDemandSubscribedItem getItem(String itemName) {
            return items.get(itemName);
        }

        /**
         * Removes the item with the given canonical name from this collection.
         *
         * @param itemName the canonical name of the item to remove
         * @return an {@link Optional} containing the removed {@link OnDemandSubscribedItem}, or
         *     empty if no item with the given name was present
         */
        public Optional<OnDemandSubscribedItem> removeItem(String itemName) {
            return Optional.ofNullable(items.remove(itemName));
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
        public Collection<SubscribedItem> values() {
            return Collections.unmodifiableCollection(items.values());
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
         * Checks whether any configured template matches the given schema.
         *
         * @param schema the {@link Schema} to test
         * @return {@code true} if at least one template matches, {@code false} otherwise
         */
        boolean matches(Schema schema);

        /**
         * Returns the topic configurations underlying the configured templates.
         *
         * @return the set of {@link TopicConfiguration}s covered by the templates
         */
        Set<TopicConfiguration> topicConfigurations();

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
        default Set<String> topicNames() {
            return topicConfigurations().stream().map(TopicConfiguration::topic).collect(toSet());
        }

        /**
         * Returns the set of topics that have at least one template matching the given item's
         * schema.
         *
         * @param schema the {@link Schema} to match against configured templates
         * @return the set of topic names whose templates match the schema
         */
        Set<String> topicsFor(Schema schema);

        /**
         * Indicates whether regex-based topic matching is enabled.
         *
         * @return {@code true} if regex matching is enabled, {@code false} otherwise
         */
        boolean isRegexEnabled();

        /**
         * Returns the compiled subscription pattern when regex topic matching is enabled.
         *
         * @return an {@link Optional} containing the compiled {@link Pattern}, or empty if regex is
         *     disabled
         */
        Optional<Pattern> subscriptionPattern();
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
                                        // Wrap each user-supplied pattern in a non-capturing
                                        // group so top-level '|' alternation preserves each
                                        // pattern's precedence without allocating capture indices.
                                        .map(t -> "(?:%s)".formatted(t.topic().topic()))
                                        .distinct()
                                        .sorted() // Only helps to simplify unit tests
                                        .collect(joining("|"))));
            }
            return Optional.empty();
        }

        @Override
        public boolean matches(Schema schema) {
            return templates.stream().anyMatch(i -> i.matches(schema));
        }

        @Override
        public Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors() {
            return templates.stream()
                    .collect(
                            groupingBy(
                                    i -> i.topic().topic(),
                                    mapping(ItemTemplate::extractor, toSet())));
        }

        @Override
        public Set<TopicConfiguration> topicConfigurations() {
            return templates.stream().map(ItemTemplate::topic).collect(toSet());
        }

        @Override
        public Set<String> topicsFor(Schema schema) {
            return templates.stream()
                    .filter(t -> t.matches(schema))
                    .map(t -> t.topic().topic())
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
     * Associates a topic with a {@link CanonicalItemExtractor} and the resulting {@link Schema} for
     * template matching. Internal helper used by {@link DefaultItemTemplates} to hold each entry of
     * its template list.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    private static class ItemTemplate<K, V> {

        private final Schema schema;
        private final TopicConfiguration topic;
        private final CanonicalItemExtractor<K, V> extractor;

        ItemTemplate(TopicConfiguration topic, CanonicalItemExtractor<K, V> extractor) {
            this.topic = Objects.requireNonNull(topic);
            this.extractor = Objects.requireNonNull(extractor);
            this.schema = extractor.schema();
        }

        public boolean matches(Schema schema) {
            return this.schema.equals(schema);
        }

        CanonicalItemExtractor<K, V> extractor() {
            return extractor;
        }

        TopicConfiguration topic() {
            return topic;
        }
    }

    /**
     * Creates an {@link OnDemandSubscribedItem} from the given subscription expression, bound to
     * the specified handle.
     *
     * @param expression the {@link SubscriptionExpression} for the item
     * @param itemHandle the handle allocated by the Lightstreamer Server
     * @return a new {@code OnDemandSubscribedItem}
     */
    public static OnDemandSubscribedItem onDemandSubscribedFrom(
            SubscriptionExpression expression, Object itemHandle) {
        return new OnDemandSubscribedItem(expression, itemHandle);
    }

    /**
     * Creates a {@link ForceableSubscribedItem} from a canonical item name string.
     *
     * @param canonicalName the canonical Lightstreamer item name
     * @return a new {@code ForceableSubscribedItem}
     * @throws ExpressionException if the input cannot be parsed as a valid subscription expression
     */
    public static ForceableSubscribedItem forceableSubscribedFrom(String canonicalName) {
        return new ForceableSubscribedItem(Expressions.Subscription(canonicalName));
    }

    /**
     * Creates an {@link ItemTemplates} instance from the given topic configurations and selector
     * suppliers.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param topicsConfig the {@link TopicConfigurations} defining topic-to-item mappings
     * @param sSuppliers the {@link KeyValueSelectorSuppliers} used to create extractors
     * @return a new {@code ItemTemplates} instance covering all configured topic mappings
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
                                topicConfig, canonicalItemExtractor(sSuppliers, template)));
            }
        }
        return new DefaultItemTemplates<>(templates, topicsConfig.isRegexEnabled());
    }

    private Items() {}
}
