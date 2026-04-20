
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.snapshot;

import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

import org.apache.kafka.common.KafkaException;

/**
 * Strategy interface for delivering snapshot data to a newly subscribed item. Implementations
 * determine how the current state of a compacted topic is resolved and sent to the client before
 * enabling real-time event delivery.
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 */
public interface SnapshotDeliveryStrategy<K, V> {

    /** Snapshot delivery mode, selecting the strategy for delivering initial state to clients. */
    enum SnapshotMode {
        /** No snapshot — real-time events are enabled immediately on subscribe. */
        DISABLED,

        /** Ephemeral per-topic consumer reads compacted topic from beginning per batch. */
        CONSUMER,

        /** In-memory materialized view of compacted topics, loaded at init, tailed continuously. */
        CACHE
    }

    /**
     * Initializes this strategy. Called once during adapter {@code init()}, before any {@link
     * #deliverSnapshot} calls. The default implementation is a no-op.
     *
     * @throws KafkaException if a Kafka operation fails during initialization, signaling that the
     *     adapter should not start
     */
    default void init() throws KafkaException {}

    /**
     * Delivers snapshot data for the given item and finalizes the snapshot phase. After this method
     * returns (or its asynchronous work completes), the item must have received {@link
     * SubscribedItem#endOfSnapshot(EventListener) endOfSnapshot} and {@link
     * SubscribedItem#enableRealtimeEvents(EventListener) enableRealtimeEvents} calls.
     *
     * <p><b>Threading contract:</b> this method is called under the consumer lock in {@code
     * DefaultSubscriptionsHandler.incrementAndMaybeStartConsuming}. Implementations must return
     * promptly — long-running or blocking I/O will hold the lock and stall all concurrent
     * subscribe/unsubscribe operations. Asynchronous strategies should enqueue work and return
     * immediately.
     *
     * @param item the newly subscribed item to deliver the snapshot for
     * @param listener the event listener used to send snapshot events to the client
     */
    void deliverSnapshot(SubscribedItem item, EventListener listener);

    /**
     * Called when all subscribed items have been unsubscribed. Allows the strategy to cancel
     * in-progress work that is no longer needed. The default implementation is a no-op — suitable
     * for strategies whose resources are long-lived (e.g., a cache).
     */
    default void onAllItemsUnsubscribed() {}

    /** Releases all resources held by this strategy. */
    void shutdown();
}
