
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Manages item subscriptions for a Kafka connection, coordinating consumer lifecycle and snapshot
 * delivery. Implementations bridge Lightstreamer's subscribe/unsubscribe calls with the underlying
 * {@link KafkaConsumerWrapper}.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public interface SubscriptionsHandler<K, V> {

    /**
     * Creates a new {@code SubscriptionsHandler} builder.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @return a new {@link Builder}
     */
    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Returns whether the given item supports snapshot delivery. The result is used by {@code
     * KafkaConnectorDataAdapter} to determine if a subscription should be treated as a snapshot
     * subscription (Path-2) or a regular subscription (Path-1).
     *
     * @param itemName the name of the item to check
     * @return {@code true} if the item supports snapshot delivery, {@code false} otherwise
     */
    boolean isSnapshotAvailable(String itemName);

    /**
     * Sets the Lightstreamer event listener used to deliver events to clients. Must be called
     * before any {@link #subscribe} call.
     *
     * @param listener the {@link ItemEventListener} to use for event delivery
     */
    void setListener(ItemEventListener listener);

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
     */
    boolean unsubscribe(String item);

    /**
     * Abstract base providing shared infrastructure for {@link SubscriptionsHandler}
     * implementations. Owns the common fields (logger, record mapper, consumer factory) used by all
     * handler variants.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    abstract class AbstractSubscriptionsHandler<K, V> implements SubscriptionsHandler<K, V> {

        /**
         * The {@link ConnectionSpec} describing this handler's Kafka connection and mapping
         * configuration.
         */
        protected final ConnectionSpec<K, V> connectionSpec;

        /** Factory used to instantiate the underlying Kafka {@link Consumer}. */
        protected final Function<Properties, Consumer<byte[], byte[]>> consumerFactory;

        /**
         * {@link Logger} scoped to this connection, keyed by the connection name from {@link
         * ConnectionSpec#connectionName()}.
         */
        protected final Logger logger;

        /**
         * Single-thread executor on which the underlying {@link KafkaConsumerWrapper} runs its poll
         * loop.
         */
        protected final ExecutorService pool;

        /**
         * The {@link ItemEventListener} used to deliver events to Lightstreamer clients; set by
         * {@link #setListener(ItemEventListener)} before any subscription is accepted.
         */
        protected ItemEventListener eventListener;

        /**
         * Constructs the shared infrastructure from the given builder.
         *
         * @param builder the {@link Builder} whose {@code connectionSpec} and {@code
         *     consumerFactory} are copied into this instance
         */
        AbstractSubscriptionsHandler(Builder<K, V> builder) {
            this.connectionSpec = builder.connectionSpec;
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
                if (!connectionSpec.pipeline().itemTemplates().matches(schema)) {
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }
                doSubscribe(expression, itemHandle);
                logger.atInfo().log("Subscribed to item [{}]", expression.canonicalItemName());
            } catch (ExpressionException e) {
                logger.atError().setCause(e).log("Invalid subscription expression");
                throw new SubscriptionException(e.getMessage());
            }
        }

        /**
         * Subclass hook invoked by {@link #subscribe(String, Object)} after the item name has been
         * parsed and validated against the configured item templates. Implementations perform the
         * mode-specific subscription bookkeeping (e.g., installing the item into the {@link
         * SubscribedItems} collection and starting or activating the underlying consumer).
         *
         * @param expression the parsed {@link SubscriptionExpression} for the item
         * @param handle the opaque handle provided by Lightstreamer for this subscription
         * @throws SubscriptionException if the subscription cannot be installed
         */
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
         * @throws IllegalStateException if the {@link ItemEventListener} has not been set
         */
        protected KafkaConsumerWrapper<K, V> newConsumer(
                boolean eagerLifecycle, SubscribedItems subscribedItems) throws KafkaException {
            if (eventListener == null) {
                throw new IllegalStateException(
                        "ItemEventListener must be set before starting the consumer");
            }
            return new KafkaConsumerWrapper<>(
                    connectionSpec,
                    eventListener,
                    subscribedItems,
                    consumerFactory,
                    eagerLifecycle);
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
     * <p>Used when {@code item.snapshot.enabled.mode} is set to {@code NONE} (snapshot disabled).
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    class OnDemandSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        /**
         * Serializes structural transitions (start/stop of the underlying consumer) against
         * increments to {@code itemsCount}.
         */
        protected final ReentrantLock consumerLock = new ReentrantLock();

        /**
         * The underlying {@link KafkaConsumerWrapper}, non-{@code null} while at least one item is
         * subscribed. Guarded by {@link #consumerLock}.
         */
        protected KafkaConsumerWrapper<K, V> consumer;

        /**
         * Latest lifecycle status of the underlying consumer, or {@code null} before the first
         * start. Guarded by {@link #consumerLock}.
         */
        protected FutureStatus lifecycleStatus;

        // Only for testing purposes: hook invoked before acquiring lock in
        // decrementAndMaybeStopConsuming().
        Runnable stopConsumingHook = () -> {};

        private final MetadataListener metadataListener;
        private final OnDemandSubscribedItems subscribedItems;
        private int itemsCount; // guarded by consumerLock

        /**
         * Constructs an {@code OnDemandSubscriptionsHandler} from the given builder.
         *
         * @param builder the {@link Builder} whose {@code metadataListener} is copied into this
         *     instance, in addition to the fields inherited via {@link
         *     AbstractSubscriptionsHandler#AbstractSubscriptionsHandler(Builder)}
         */
        OnDemandSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
            this.metadataListener = builder.metadataListener;
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
                logger.atError().setCause(e).log("Invalid subscription expression");
                throw new SubscriptionException(e.getMessage());
            }
        }

        @Override
        public boolean isSnapshotAvailable(String itemName) {
            return false;
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
                    lifecycleStatus =
                            consumer.start(
                                    pool, cause -> metadataListener.forceUnsubscriptionAll());
                    if (lifecycleStatus.initFailed()) {
                        logger.atError()
                                .log("Consumer initialization failed: {}", lifecycleStatus.join());
                        metadataListener.forceUnsubscriptionAll();
                    }

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
        boolean isConsuming() {
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
     * <p>Used when {@code item.snapshot.enabled.mode} is set to any value other than {@code NONE}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    class ForceableSubscriptionsHandler<K, V> extends AbstractSubscriptionsHandler<K, V> {

        private final long itemSnapshotMaxIdleSeconds;
        private final ScheduledExecutorService idleSnapshotScheduler;
        private ForceableSubscribedItems subscribedItems;
        private FutureStatus lifecycleStatus;
        private Optional<ScheduledFuture<?>> scheduled = Optional.empty();

        /**
         * Constructs a {@code ForceableSubscriptionsHandler} from the given builder.
         *
         * @param builder the {@link Builder} whose {@code itemSnapshotMaxIdleSeconds} is copied
         *     into this instance, in addition to the fields inherited via {@link
         *     AbstractSubscriptionsHandler#AbstractSubscriptionsHandler(Builder)}
         */
        ForceableSubscriptionsHandler(Builder<K, V> builder) {
            super(builder);
            this.itemSnapshotMaxIdleSeconds = builder.itemSnapshotMaxIdleSeconds;
            this.idleSnapshotScheduler =
                    Executors.newScheduledThreadPool(
                            1,
                            r -> {
                                Thread t = new Thread(r, "IdleSnapshotScheduler");
                                t.setDaemon(true);
                                return t;
                            });
        }

        @Override
        protected void doSetListener(ItemEventListener listener) {
            this.subscribedItems = SubscribedItems.forceable(listener, logger);
            startConsuming();
            if (itemSnapshotMaxIdleSeconds > 0) {
                long checkPeriodSeconds = Math.max(1, itemSnapshotMaxIdleSeconds / 2);
                logger.atInfo().log(
                        "Scheduling snapshot idle-expiration check every {} s (max idle {} s)",
                        checkPeriodSeconds,
                        itemSnapshotMaxIdleSeconds);
                this.scheduled = Optional.of(scheduleIdleSnapshotCheck(checkPeriodSeconds));
            }
        }

        private ScheduledFuture<?> scheduleIdleSnapshotCheck(long checkPeriodSeconds) {
            return this.idleSnapshotScheduler.scheduleWithFixedDelay(
                    () -> {
                        try {
                            subscribedItems.clearIdleSnapshots(itemSnapshotMaxIdleSeconds);
                        } catch (Throwable t) {
                            logger.atError()
                                    .setCause(t)
                                    .log("Snapshot idle-expiration check failed");
                        }
                    },
                    checkPeriodSeconds,
                    checkPeriodSeconds,
                    TimeUnit.SECONDS);
        }

        /** Starts the Kafka consumer eagerly. Called once during initialization. */
        private void startConsuming() {
            logger.atInfo().log("Starting consumer eagerly for forced subscriptions support...");
            KafkaConsumerWrapper<K, V> consumer;
            try {
                consumer = newConsumer(true, subscribedItems);
            } catch (KafkaException ke) {
                logger.atError().setCause(ke).log("Unable to connect to Kafka");
                throw ke;
            }
            this.lifecycleStatus = consumer.start(pool, eventListener::failure);
            if (lifecycleStatus.initFailed()) {
                throw new KafkaException(
                        "Consumer initialization failed: " + lifecycleStatus.join());
            }
            logger.atInfo().log("Consumer started");
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

        // Only for testing purposes
        ForceableSubscribedItems getSubscribedItems() {
            return subscribedItems;
        }

        // Only for testing purposes
        FutureStatus getLifecycleStatus() {
            return lifecycleStatus;
        }

        // Only for testing purposes
        long getItemSnapshotMaxIdleSeconds() {
            return itemSnapshotMaxIdleSeconds;
        }

        // Only for testing purposes
        Optional<ScheduledFuture<?>> getScheduled() {
            return scheduled;
        }
    }

    /**
     * Builder for creating {@link SubscriptionsHandler} instances. The implementation is selected
     * based on whether snapshot support is enabled (see {@link #snapshotEnabled(boolean)}): when
     * enabled a {@link ForceableSubscriptionsHandler} is returned; otherwise an {@link
     * OnDemandSubscriptionsHandler}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    static class Builder<K, V> {

        private Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
        private ConnectionSpec<K, V> connectionSpec;
        private MetadataListener metadataListener;
        private long itemSnapshotMaxIdleSeconds = 0;
        private boolean snapshotEnabled;

        private Builder() {}

        /**
         * Sets the factory used to create the underlying Kafka {@link Consumer}.
         *
         * @param consumerFactory the consumer factory
         * @return this builder
         */
        public Builder<K, V> consumerFactory(
                Function<Properties, Consumer<byte[], byte[]>> consumerFactory) {
            this.consumerFactory = consumerFactory;
            return this;
        }

        /**
         * Sets the {@link ConnectionSpec} describing the Kafka connection and its mapping
         * configuration.
         *
         * @param connectionSpec the connection spec
         * @return this builder
         */
        public Builder<K, V> connectionSpec(ConnectionSpec<K, V> connectionSpec) {
            this.connectionSpec = connectionSpec;
            return this;
        }

        /**
         * Sets the {@link MetadataListener} used to force unsubscriptions when the consumer cannot
         * recover. Required when snapshot support is disabled.
         *
         * @param metadataListener the metadata listener
         * @return this builder
         */
        public Builder<K, V> metadataListener(MetadataListener metadataListener) {
            this.metadataListener = metadataListener;
            return this;
        }

        /**
         * Selects the handler implementation: {@code true} returns a {@link
         * ForceableSubscriptionsHandler} with eager consumer lifecycle and snapshot support; {@code
         * false} returns an {@link OnDemandSubscriptionsHandler}.
         *
         * @param snapshotEnabled {@code true} to enable snapshot support, {@code false} otherwise
         * @return this builder
         */
        public Builder<K, V> snapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
            return this;
        }

        /**
         * Sets the maximum idle time, in seconds, after which a snapshotted item that has received
         * no record-driven access is considered stale and a {@code clearSnapshot} is pushed to the
         * Lightstreamer kernel. A value of {@code 0} disables the sliding-expiration check.
         *
         * @param itemSnapshotMaxIdleSeconds the max idle in seconds; must be non-negative
         * @return this builder
         */
        public Builder<K, V> itemSnapshotMaxIdleSeconds(long itemSnapshotMaxIdleSeconds) {
            this.itemSnapshotMaxIdleSeconds = itemSnapshotMaxIdleSeconds;
            return this;
        }

        /**
         * Builds the configured {@link SubscriptionsHandler}.
         *
         * @return a new {@link SubscriptionsHandler} instance
         * @throws IllegalStateException if a required builder property has not been set, or if
         *     {@code itemSnapshotMaxIdleSeconds} is negative
         */
        public SubscriptionsHandler<K, V> build() {
            if (consumerFactory == null) {
                throw new IllegalStateException("ConsumerFactory not set");
            }

            if (connectionSpec == null) throw new IllegalStateException("ConnectionSpec not set");
            if (snapshotEnabled) {
                if (itemSnapshotMaxIdleSeconds < 0) {
                    throw new IllegalStateException(
                            "itemSnapshotMaxIdleSeconds must be non-negative");
                }
                return new ForceableSubscriptionsHandler<>(this);
            }
            if (metadataListener == null) {
                throw new IllegalStateException("MetadataListener not set");
            }

            return new OnDemandSubscriptionsHandler<>(this);
        }
    }
}
