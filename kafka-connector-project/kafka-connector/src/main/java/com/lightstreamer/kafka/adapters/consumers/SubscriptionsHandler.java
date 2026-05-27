
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

package com.lightstreamer.kafka.adapters.consumers;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus;
import com.lightstreamer.kafka.adapters.consumers.KafkaConsumerWrapper.FutureStatus.State;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ForceableSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Manages item subscriptions for a Kafka connection, coordinating consumer lifecycle and snapshot
 * delivery. Implementations bridge Lightstreamer's subscribe/unsubscribe calls with the underlying
 * {@link KafkaConsumerWrapper}.
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 */
public interface SubscriptionsHandler<K, V> {

    /**
     * Subscribes to the given item, starting the Kafka consumer if required by the implementation.
     *
     * @param item the item name to subscribe to
     * @param itemHandle the opaque handle provided by Lightstreamer for this subscription
     * @throws SubscriptionException if the item does not match any configured templates
     */
    void subscribe(String item, Object itemHandle) throws SubscriptionException;

    /**
     * Unsubscribes from the given item, stopping the Kafka consumer if no active subscriptions
     * remain.
     *
     * @param item the item name to unsubscribe from
     * @return {@code true} if the item was unsubscribed, {@code false} if the item was not
     *     subscribed
     * @throws SubscriptionException if the unsubscription fails
     */
    boolean unsubscribe(String item) throws SubscriptionException;

    /**
     * Returns whether the given item supports snapshot delivery. The result is used by {@code
     * KafkaConnectorDataAdapter} to determine if a subscription should be treated as a snapshot
     * subscription (Path-2) or a regular subscription (Path-1).
     *
     * @param itemName the name of the item to check
     * @return {@code true} if the item supports snapshot delivery, {@code false} otherwise
     * @throws SubscriptionException if the item does not match any configured templates
     */
    boolean isSnapshotAvailable(String itemName);

    /**
     * Returns whether a Kafka consumer is currently active and consuming events.
     *
     * @return {@code true} if the consumer is running, {@code false} otherwise
     */
    boolean isConsuming();

    /**
     * Sets the Lightstreamer event listener used to deliver events to clients. Must be called
     * before any {@link #subscribe} call.
     *
     * @param listener the {@link ItemEventListener} to use for event delivery
     */
    void setListener(ItemEventListener listener);

    /**
     * Creates a new {@code SubscriptionsHandler} builder.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     * @return a new {@link Builder}
     */
    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Builder for creating {@link SubscriptionsHandler} instances. The implementation is selected
     * from the connection's {@code item.snapshot.enable} flag: when {@code true} a {@link
     * ForceableSubscriptionsHandler} is returned; otherwise an {@link
     * OnDemandSubscriptionsHandler}.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     */
    static class Builder<K, V> {

        private ConnectionSpec<K, V> connectionSpec;
        private MetadataListener metadataListener;
        private Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
        private boolean itemSnapshotEnabled = false;

        private Builder() {}

        public Builder<K, V> withConnectionSpec(ConnectionSpec<K, V> connectionSpec) {
            this.connectionSpec = connectionSpec;
            return this;
        }

        public Builder<K, V> withMetadataListener(MetadataListener metadataListener) {
            this.metadataListener = metadataListener;
            return this;
        }

        public Builder<K, V> withConsumerFactory(
                Function<Properties, Consumer<byte[], byte[]>> consumerFactory) {
            this.consumerFactory = consumerFactory;
            return this;
        }

        public Builder<K, V> withItemSnapshotEnabled(boolean itemSnapshotEnabled) {
            this.itemSnapshotEnabled = itemSnapshotEnabled;
            return this;
        }

        public SubscriptionsHandler<K, V> build() {
            if (connectionSpec == null) throw new IllegalStateException("ConnectionSpec not set");
            if (connectionSpec.commandMode().manageSnapshot() && itemSnapshotEnabled) {
                throw new IllegalStateException(
                        "Invalid configuration: command mode "
                                + connectionSpec.commandMode()
                                + " is not compatible with item snapshot enablement");
            }
            if (metadataListener == null)
                throw new IllegalStateException("MetadataListener not set");
            if (consumerFactory == null) throw new IllegalStateException("ConsumerFactory not set");
            return itemSnapshotEnabled
                    ? new ForceableSubscriptionsHandler<>(this)
                    : new OnDemandSubscriptionsHandler<>(this);
        }
    }

    /**
     * Abstract base providing shared infrastructure for {@link SubscriptionsHandler}
     * implementations. Owns the common fields (logger, record mapper, consumer factory) used by all
     * handler variants.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     */
    abstract class AbstractSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        protected final ConnectionSpec<K, V> connectionSpec;
        protected final MetadataListener metadataListener;
        protected final Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
        protected final Logger logger;
        protected final ExecutorService pool;

        protected final ReentrantLock consumerLock = new ReentrantLock();

        protected KafkaConsumerWrapper<K, V> consumer; // guarded by consumerLock
        protected FutureStatus lifecycleStatus; // guarded by consumerLock

        protected ItemEventListener eventListener;

        /** Constructs the shared infrastructure from the given builder. */
        AbstractSubscriptionsHandler(Builder<K, V> builder) {
            this.connectionSpec = builder.connectionSpec;
            this.metadataListener = builder.metadataListener;
            this.consumerFactory = builder.consumerFactory;
            this.logger = LogFactory.getLogger(connectionSpec.connectionName());
            this.pool =
                    Executors.newSingleThreadExecutor(r -> new Thread(r, "SubscriptionHandler"));
        }

        @Override
        public final void subscribe(String item, Object itemHandle) throws SubscriptionException {
            try {
                SubscriptionExpression expression = Expressions.Subscription(item);
                Schema schema = expression.schema();
                if (!connectionSpec.itemTemplates().matches(schema)) {
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }
                doSubscribe(expression, itemHandle);
                logger.atInfo().log("Subscribed to item [{}]", expression.canonicalItemName());
            } catch (ExpressionException e) {
                logger.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        abstract void doSubscribe(SubscriptionExpression expression, Object handle)
                throws SubscriptionException;

        @Override
        public final void setListener(ItemEventListener listener) {
            if (listener == null) {
                throw new IllegalArgumentException("ItemEventListener cannot be null");
            }
            this.eventListener = listener;
            doSetListener(eventListener);
        }

        /**
         * Hook invoked by {@link #setListener(ItemEventListener)} after null-checking the listener.
         * Subclasses initialize the {@link ItemEventListener} and perform any mode-specific setup
         * (e.g., initializing a snapshot strategy or starting the consumer).
         *
         * @param listener the non-null {@code ItemEventListener} provided by Lightstreamer
         */
        protected void doSetListener(ItemEventListener listener) {}

        /**
         * Creates a new {@link KafkaConsumerWrapper} configured for this handler's connection.
         *
         * @param eagerLifecycle {@code true} for an eager consumer, {@code false} for on-demand
         * @param subscribedItems the {@link SubscribedItems} collection the consumer should route
         *     records into
         * @return a new {@code KafkaConsumerWrapper} instance
         * @throws KafkaException if the consumer cannot be created
         */
        protected KafkaConsumerWrapper<K, V> newConsumer(
                boolean eagerLifecycle, SubscribedItems subscribedItems) throws KafkaException {
            if (eventListener == null) {
                throw new RuntimeException(
                        "ItemEventListener must be set before starting the consumer");
            }
            return new KafkaConsumerWrapper<>(
                    connectionSpec,
                    metadataListener,
                    eventListener,
                    subscribedItems,
                    consumerFactory,
                    eagerLifecycle);
        }

        @Override
        public final boolean isConsuming() {
            consumerLock.lock();
            try {
                return consumer != null && !lifecycleStatus.isStateAvailable();
            } finally {
                consumerLock.unlock();
            }
        }

        /**
         * Returns the latest lifecycle state for testing, waiting for it to be resolved when a
         * status is available.
         *
         * <p>This method is intended only for tests.
         *
         * @return the resolved lifecycle {@link State}, or {@code Optional.empty()} if the consumer
         *     has never been started
         */
        Optional<State> joinCurrentState() {
            FutureStatus statusToRead;
            consumerLock.lock();
            try {
                if (lifecycleStatus == null) {
                    return Optional.empty();
                }
                statusToRead = lifecycleStatus;
            } finally {
                consumerLock.unlock();
            }

            return Optional.of(statusToRead.join());
        }

        // Only for testing purposes
        boolean isConsumerActive() {
            consumerLock.lock();
            try {
                return consumer != null;
            } finally {
                consumerLock.unlock();
            }
        }
    }

    /**
     * {@link SubscriptionsHandler} implementation that manages the Kafka consumer lifecycle on
     * demand — starting it on the first subscription and shutting it down when the last item is
     * unsubscribed.
     *
     * <ul>
     *   <li>{@link #subscribe(String, Object)} validates the item against configured templates,
     *       adds it to the active set, and starts the consumer if this is the first subscription.
     *   <li>{@link #unsubscribe(String)} removes the item from the active set and stops the
     *       consumer when no subscriptions remain.
     * </ul>
     *
     * <p>Used when {@code item.snapshot.enable} is {@code false}.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     */
    class OnDemandSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        // Only for testing purposes: hook invoked before acquiring lock in
        // decrementAndMaybeStopConsuming()
        Runnable stopConsumingHook = () -> {};

        private int itemsCount; // guarded by consumerLock
        private OnDemandSubscribedItems subscribedItems;

        /** Constructs an {@code OnDemandSubscriptionsHandler} from the given builder. */
        OnDemandSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
            this.subscribedItems = SubscribedItems.onDemand();
        }

        @Override
        void doSubscribe(SubscriptionExpression expression, Object handle)
                throws SubscriptionException {
            try {
                OnDemandSubscribedItem newItem = Items.onDemandSubscribedFrom(expression, handle);
                subscribedItems.addItem(newItem);
                incrementAndMaybeStartConsuming(newItem);
            } catch (ExpressionException e) {
                logger.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        @Override
        public boolean isSnapshotAvailable(String itemName) {
            return connectionSpec.commandMode().manageSnapshot();
        }

        /**
         * Increments the subscription count and starts the Kafka consumer if this is the first
         * subscription. Delivers the snapshot for the newly subscribed item.
         *
         * @param item the newly subscribed item
         */
        private void incrementAndMaybeStartConsuming(SubscribedItem item) {
            logger.atTrace().log("Acquiring consumer lock to start consuming events...");
            consumerLock.lock();
            logger.atTrace().log("Consumer lock acquired");
            try {
                itemsCount++;
                if (itemsCount == 1) {
                    logger.atInfo().log("Consumer not yet initialized, creating a new one...");
                    consumer = newConsumer(false, subscribedItems); // May throw KafkaException
                    logger.atInfo().log("New consumer connecting and subscribing...");
                    lifecycleStatus = consumer.start(pool);
                } else {
                    logger.atDebug().log("Consumer is already consuming events, nothing to do");
                }
            } catch (KafkaException ke) {
                logger.atError().setCause(ke).log("Unable to connect to Kafka");
                metadataListener.forceUnsubscriptionAll();
            } finally {
                logger.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                logger.atTrace().log("Consumer lock released");
            }
        }

        @Override
        public boolean unsubscribe(String item) {
            boolean removed = subscribedItems.removeItem(item).isPresent();
            if (removed) {
                decrementAndMaybeStopConsuming();
            }

            return removed;
        }

        /**
         * Decrements the subscription count and stops the Kafka consumer if no active subscriptions
         * remain.
         */
        private void decrementAndMaybeStopConsuming() {
            stopConsumingHook.run();
            logger.atTrace().log("Acquiring consumer lock to stop consuming...");
            consumerLock.lock();
            logger.atTrace().log("Consumer lock acquired to stop consuming");
            try {
                itemsCount--;
                if (itemsCount == 0) {
                    if (consumer != null) {
                        logger.atInfo().log("Stopping consumer...");
                        consumer.shutdown();
                        consumer = null;
                        logger.atInfo().log("Consumer stopped");
                    } else {
                        logger.atDebug().log("Consumer was not initialized, nothing to do");
                    }
                } else {
                    logger.atDebug().log("Consumer still has active subscriptions, nothing to do");
                }
            } finally {
                logger.atTrace().log("Releasing consumer lock...");
                consumerLock.unlock();
                logger.atTrace().log("Consumer lock released");
            }
        }

        // Only for testing purposes
        int getItemsCounter() {
            consumerLock.lock();
            try {
                return itemsCount;
            } finally {
                consumerLock.unlock();
            }
        }

        // Only for testing purposes
        OnDemandSubscribedItems getSubscribedItems() {
            return subscribedItems;
        }
    }

    /**
     * {@link SubscriptionsHandler} implementation that manages the Kafka consumer lifecycle eagerly
     * — starting it during {@link #setListener(ItemEventListener)} initialization and keeping it
     * running for the adapter's entire lifetime.
     *
     * <ul>
     *   <li>{@link #subscribe(String, Object)} either binds the Server-allocated handle on a
     *       pre-existing entry (Path-2: previously installed by a record-driven cache miss in
     *       {@link Items.ForceableSubscribedItems#getItem(String)}) or installs a fresh {@link
     *       Items.SubscribedItem} (Path-1: organic, client-driven). Path-1 entries remain unforced
     *       until a record arrives; Path-2 entries are forced from the moment {@code getItem}
     *       drives {@code forceSubscription(name)}.
     *   <li>{@link #unsubscribe(String)} prunes Path-1 entries that were never forced; forced
     *       entries remain eternal so any future client subscription receives the current state as
     *       a snapshot.
     * </ul>
     *
     * <p>Used when {@code item.snapshot.enable} is {@code true}.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     */
    class ForceableSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        private ForceableSubscribedItems subscribedItems;

        /** Constructs a {@code ForceableSubscriptionsHandler} from the given builder. */
        ForceableSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
        }

        @Override
        protected void doSetListener(ItemEventListener listener) {
            this.subscribedItems = SubscribedItems.forceable(listener, logger);
            startConsuming();
        }

        ItemEventListener getEventListener() {
            return eventListener;
        }

        /** Starts the Kafka consumer eagerly. Called once during initialization. */
        private void startConsuming() {
            consumerLock.lock();
            try {
                logger.atInfo().log(
                        "Starting consumer eagerly for forced subscriptions support...");
                try {
                    consumer = newConsumer(true, subscribedItems);
                } catch (KafkaException ke) {
                    logger.atError().setCause(ke).log("Unable to connect to Kafka");
                    throw ke;
                }
                lifecycleStatus = consumer.start(pool);
                if (lifecycleStatus.initFailed()) {
                    throw new KafkaException(
                            "Consumer initialization failed: " + lifecycleStatus.join());
                }
                logger.atInfo().log("Consumer started");
            } finally {
                consumerLock.unlock();
            }
        }

        @Override
        void doSubscribe(SubscriptionExpression expression, Object handle)
                throws SubscriptionException {
            // Atomic install-or-activate: installs a fresh entry and emits endOfSnapshot on
            // Path-1 (organic); on Path-2 activates the existing placeholder installed by the
            // poll thread (currently blocked in forceSubscription waiting for us).
            subscribedItems.activateOrInstall(expression, handle);
        }

        @Override
        public boolean unsubscribe(String item) {
            // Forced (Path-2) entries are eternal: the eager consumer keeps feeding them so any
            // future client subscription receives the current state as a snapshot. Path-1 entries
            // that never received a record (and therefore were never promoted to eternal via
            // forceSubscription) are pruned here so the map does not accumulate stale entries
            // carrying handles the SDK has already torn down.
            return subscribedItems.removeIfUnforced(item);
        }

        @Override
        public boolean isSnapshotAvailable(String itemName) {
            return true;
        }
    }
}
