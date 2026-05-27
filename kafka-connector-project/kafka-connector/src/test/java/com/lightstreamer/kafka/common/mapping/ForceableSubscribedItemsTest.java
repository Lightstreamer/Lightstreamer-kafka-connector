
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.Mocks.EventCall.EventType.CS;
import static com.lightstreamer.kafka.test_utils.Mocks.EventCall.EventType.EOS;
import static com.lightstreamer.kafka.test_utils.Mocks.EventCall.EventType.UPDATE;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.common.mapping.Items.BufferedSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.ForceableSubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link ForceableSubscribedItems}.
 *
 * <p>Covers the forceable / per-name-lock pipeline contract:
 *
 * <ul>
 *   <li>{@link ForceableSubscribedItems#activateOrInstall(SubscriptionExpression, Object)} is the
 *       single install path; it installs a fresh {@code BufferedSubscribedItem} immediately
 *       switched to direct-dispatch mode (Path-1 organic), or activates the existing placeholder
 *       (Path-2) by draining it, switching it to direct dispatch, and marking it forced.
 *   <li>{@link ForceableSubscribedItems#getItem(String)} is side-effecting: on a miss, it installs
 *       a placeholder, triggers {@code forceSubscription}, and returns the forced entry; on a hit
 *       against an unforced entry, it promotes it to forced; and on a hit against a forced entry,
 *       it returns it lock-free from a fast path.
 *   <li>{@link ForceableSubscribedItems#removeIfUnforced(String)} removes only unforced entries;
 *       forced (eternal) entries are not removed.
 * </ul>
 */
public class ForceableSubscribedItemsTest {

    private ForceableSubscribedItems items;
    private MockItemEventListener listener;

    @BeforeEach
    public void setUp() {
        final Logger logger = LogFactory.getLogger("ForceableSubscribedItemsTest");
        listener = new MockItemEventListener();
        items = Items.SubscribedItems.forceable(listener, logger);
    }

    @Test
    public void shouldRejectNullListenerSupplier() {
        final Logger logger = LogFactory.getLogger("ForceableSubscribedItemsTest");
        assertThrows(
                NullPointerException.class,
                () -> Items.SubscribedItems.forceable(null, logger),
                "listener cannot be null");
    }

    @Test
    public void shouldStartEmpty() {
        assertThat(items.isEmpty()).isTrue();
        assertThat(items.size()).isEqualTo(0);
        assertThat(items.values()).isEmpty();
    }

    /** removeIfUnforced removes unforced entries and keeps forced (eternal) entries. */
    @Test
    public void shouldRemoveUnforcedAndKeepForcedEntries() {
        final Object forcedHandle = new Object();
        final Object unforcedHandle = new Object();

        // Add one forced entry and one unforced entry.
        listener.setForceSubscriptionAction(
                name -> {
                    if ("eternal".equals(name)) {
                        items.activateOrInstall(Expressions.Subscription("eternal"), forcedHandle);
                    }
                });
        BufferedSubscribedItem forced = items.getItem("eternal");
        BufferedSubscribedItem unforced =
                items.activateOrInstall(Expressions.Subscription("item2"), unforcedHandle);

        assertThat(forced.isForced()).isTrue();
        assertThat(unforced.isForced()).isFalse();
        assertThat(items.size()).isEqualTo(2);

        // removeIfUnforced removes the unforced entry.
        var removedUnforced = items.removeIfUnforced("item2");
        assertThat(removedUnforced).isTrue();
        assertThat(items.size()).isEqualTo(1);

        // removeIfUnforced does not remove the forced entry.
        var removedForced = items.removeIfUnforced("eternal");
        assertThat(removedForced).isFalse();
        assertThat(items.size()).isEqualTo(1);

        // Verify forced entry is still accessible.
        BufferedSubscribedItem still = items.getItem("eternal");
        assertThat(still).isNotNull();
        assertThat(still.isForced()).isTrue();
    }

    /**
     * Path-1 (organic): activateOrInstall installs a fresh entry directly in direct-dispatch mode
     * and emits endOfSnapshot.
     */
    @Test
    public void shouldAddItemPath1Organic() {
        final Object itemHandle = new Object();

        BufferedSubscribedItem added =
                items.activateOrInstall(
                        Expressions.Subscription("stock-[symbol=AAPL]"), itemHandle);
        assertThat(added).isNotNull();
        assertThat(added.canonicalName()).isEqualTo("stock-[symbol=AAPL]");
        assertThat(added.isForced()).isFalse(); // Path-1 entries start unforced
        assertThat(items.size()).isEqualTo(1);

        // endOfSnapshot should be emitted for the fresh Path-1 entry
        assertThat(listener.getSmartEndOfSnapshotCalls()).hasSize(1);
        assertThat(listener.getSmartEndOfSnapshotCalls().get(0)).isEqualTo(itemHandle);

        // Path-1 activation also binds direct dispatch to itemHandle.
        added.clearSnapshot(listener);
        assertThat(listener.getSmartClearSnapshotCalls()).containsExactly(itemHandle);
    }

    /**
     * Path-2 (forceable) case (1): getItem on a true miss installs a placeholder, triggers
     * forceSubscription (Server callback activates it), and returns the forced entry.
     */
    @Test
    public void shouldGetItemPath2Case1TrueMiss() {
        final Object itemHandle = new Object();
        AtomicBoolean forceSubscriptionCalled = new AtomicBoolean(false);

        listener.setForceSubscriptionAction(
                name -> {
                    if ("orders-[k=v]".equals(name)) {
                        forceSubscriptionCalled.set(true);
                        // Server callback activates the placeholder (case A)
                        var buffered =
                                items.activateOrInstall(
                                        Expressions.Subscription("orders-[k=v]"), itemHandle);
                        // Case A returns null (activation of existing placeholder)
                        assertThat(buffered).isNull();
                    }
                });

        // getItem on miss installs placeholder and triggers forceSubscription
        BufferedSubscribedItem item = items.getItem("orders-[k=v]");
        assertThat(item).isNotNull();
        assertThat(item.canonicalName()).isEqualTo("orders-[k=v]");
        assertThat(item.isForced()).isTrue();
        assertThat(forceSubscriptionCalled.get()).isTrue();
        assertThat(items.size()).isEqualTo(1);

        // No endOfSnapshot emitted for Path-2 (virtual handle has no client)
        assertThat(listener.getSmartEndOfSnapshotCalls()).isEmpty();

        // Path-2 activation switches the placeholder to direct dispatch bound to itemHandle.
        item.clearSnapshot(listener);
        assertThat(listener.getSmartClearSnapshotCalls()).containsExactly(itemHandle);
    }

    /**
     * Path-2 (forceable) case (2): getItem on a hit against an unforced entry promotes it to forced
     * via forceSubscription and is idempotent on re-call.
     */
    @Test
    public void shouldGetItemPath2Case2HitOnUnforced() {
        final Object itemHandle = new Object();

        // Path-1 organic: add unforced entry
        BufferedSubscribedItem unforced =
                items.activateOrInstall(Expressions.Subscription("stock-[symbol=IBM]"), itemHandle);
        assertThat(unforced.isForced()).isFalse();
        assertThat(items.size()).isEqualTo(1);

        // endOfSnapshot should be emitted for the fresh Path-1 entry
        assertThat(listener.getSmartEndOfSnapshotCalls()).hasSize(1);
        assertThat(listener.getSmartEndOfSnapshotCalls().get(0)).isEqualTo(itemHandle);
        listener.reset();

        AtomicBoolean forceSubscriptionCalled = new AtomicBoolean(false);
        listener.setForceSubscriptionAction(
                name -> {
                    if ("stock-[symbol=IBM]".equals(name)) {
                        forceSubscriptionCalled.set(true);
                        // Server treats second call as no-op (C-fs-noop): no callback fired
                        // The entry is already in the map and will be promoted by the
                        // hit-on-unforced
                        // path
                    }
                });

        // getItem on hit-on-unforced: promotes to forced (case 2)
        BufferedSubscribedItem promoted = items.getItem("stock-[symbol=IBM]");
        assertThat(promoted).isSameInstanceAs(unforced);
        assertThat(promoted.isForced()).isTrue();
        assertThat(forceSubscriptionCalled.get()).isTrue();

        listener.reset();

        // Subsequent getItem: lock-free fast path, no forceSubscription call
        BufferedSubscribedItem cached = items.getItem("stock-[symbol=IBM]");
        assertThat(cached).isSameInstanceAs(promoted);
        assertThat(listener.getSmartEndOfSnapshotCalls()).isEmpty(); // No new callbacks
    }

    /**
     * Concurrent hit-on-unforced promotion: two callers race on the same organic Path-1 entry and
     * converge to one forced instance.
     */
    @Test
    public void shouldCoverConcurrentCase2HitOnUnforcedPromotion() throws Exception {
        final String itemName = "path1-race-[k=v]";
        final Object itemHandle = new Object();

        BufferedSubscribedItem unforced =
                items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
        assertThat(unforced.isForced()).isFalse();

        CountDownLatch callbackEntered = new CountDownLatch(2);
        CountDownLatch allowReturn = new CountDownLatch(1);
        AtomicInteger forceCalls = new AtomicInteger(0);

        listener.setForceSubscriptionAction(
                name -> {
                    if (!itemName.equals(name)) {
                        return;
                    }
                    forceCalls.incrementAndGet();
                    callbackEntered.countDown();
                    try {
                        boolean released = allowReturn.await(3, TimeUnit.SECONDS);
                        assertThat(released).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<BufferedSubscribedItem> firstRef = new AtomicReference<>();
        AtomicReference<BufferedSubscribedItem> secondRef = new AtomicReference<>();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();

        Thread t1 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                firstRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                firstError.set(t);
                            }
                        },
                        "forceable-hit-unforced-race-1");

        Thread t2 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                secondRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                secondError.set(t);
                            }
                        },
                        "forceable-hit-unforced-race-2");

        t1.start();
        t2.start();
        startGate.countDown();

        assertThat(callbackEntered.await(3, TimeUnit.SECONDS)).isTrue();
        allowReturn.countDown();

        t1.join(TimeUnit.SECONDS.toMillis(3));
        t2.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(t1.isAlive()).isFalse();
        assertThat(t2.isAlive()).isFalse();
        assertThat(firstError.get()).isNull();
        assertThat(secondError.get()).isNull();

        BufferedSubscribedItem first = firstRef.get();
        BufferedSubscribedItem second = secondRef.get();
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first).isSameInstanceAs(second);
        assertThat(first).isSameInstanceAs(unforced);
        assertThat(first.isForced()).isTrue();

        assertThat(items.size()).isEqualTo(1);
        assertThat(forceCalls.get()).isEqualTo(2);
    }

    /**
     * Path-2 (forceable) case (0): Multiple fast-path reads on a forced entry return the same
     * instance without locking.
     */
    @Test
    public void shouldGetItemPath2Case0FastPathLockFree() {
        final Object itemHandle = new Object();

        // Install and force the entry
        listener.setForceSubscriptionAction(
                name -> {
                    if ("orders-[k=v]".equals(name)) {
                        items.activateOrInstall(
                                Expressions.Subscription("orders-[k=v]"), itemHandle);
                    }
                });
        BufferedSubscribedItem first = items.getItem("orders-[k=v]");
        assertThat(first.isForced()).isTrue();

        listener.reset();

        // Multiple subsequent reads should all return the same instance (lock-free fast path)
        BufferedSubscribedItem second = items.getItem("orders-[k=v]");
        assertThat(second).isSameInstanceAs(first);

        BufferedSubscribedItem third = items.getItem("orders-[k=v]");
        assertThat(third).isSameInstanceAs(first);

        // No forceSubscription calls on fast-path hits
        assertThat(listener.getSmartEndOfSnapshotCalls()).isEmpty();
    }

    /*
     * Concurrent race catalog mapping:
     * - shouldCoverConcurrentSameNameConvergenceBoundedForceCalls:
     *   same-name concurrent getItem (general convergence, bounded forceSubscription count).
     * - shouldCoverConcurrentCase2HitOnUnforcedPromotion:
     *   case (2) hit-on-unforced under concurrent callers.
     * - shouldCoverConcurrentActivationRaceWithOrderedQueuedDrain:
     *   activation race with pre-activation buffered UPDATE/CS/EOS/UPDATE drain ordering.
     * - shouldCoverConcurrentActivationBoundaryWithoutLossOrDuplication:
     *   producer traffic spanning queueing-to-direct transition (no loss/duplication).
     * - shouldCoverConcurrentCase1BothCallersReachSlowPathBeforeActivation:
     *   deterministic race-lost setup where both callers cross slow path before activation.
     * - shouldCoverConcurrentCase0FastPathWhileFirstCallerUnwinds:
     *   mixed timing interleaving: first caller still unwinding, second caller uses fast path.
     */

    /**
     * Concurrent same-name getItem calls converge to one forced entry and never duplicate the map
     * entry.
     */
    @Test
    public void shouldCoverConcurrentSameNameConvergenceBoundedForceCalls() throws Exception {
        final String itemName = "race-[k=v]";
        final Object itemHandle = new Object();

        AtomicInteger forceCalls = new AtomicInteger(0);
        listener.setForceSubscriptionAction(
                name -> {
                    if (itemName.equals(name)) {
                        forceCalls.incrementAndGet();
                        items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
                    }
                });

        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<BufferedSubscribedItem> firstRef = new AtomicReference<>();
        AtomicReference<BufferedSubscribedItem> secondRef = new AtomicReference<>();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();

        Thread t1 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                firstRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                firstError.set(t);
                            }
                        },
                        "forceable-getItem-race-1");

        Thread t2 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                secondRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                secondError.set(t);
                            }
                        },
                        "forceable-getItem-race-2");

        t1.start();
        t2.start();
        startGate.countDown();

        t1.join(TimeUnit.SECONDS.toMillis(3));
        t2.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(t1.isAlive()).isFalse();
        assertThat(t2.isAlive()).isFalse();
        assertThat(firstError.get()).isNull();
        assertThat(secondError.get()).isNull();

        BufferedSubscribedItem first = firstRef.get();
        BufferedSubscribedItem second = secondRef.get();
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first).isSameInstanceAs(second);
        assertThat(first.isForced()).isTrue();

        assertThat(items.size()).isEqualTo(1);
        assertThat(items.getItem(itemName)).isSameInstanceAs(first);
        assertThat(forceCalls.get()).isAtLeast(1);
        assertThat(forceCalls.get()).isAtMost(2);
    }

    /**
     * Concurrent activation race: events queued before handle binding are drained in insertion
     * order on activation and delivered against the bound handle.
     */
    @Test
    public void shouldCoverConcurrentActivationRaceWithOrderedQueuedDrain() throws Exception {
        final String itemName = "queue-race-[k=v]";
        final Object itemHandle = new Object();
        final CountDownLatch activationEntered = new CountDownLatch(1);
        final CountDownLatch allowActivation = new CountDownLatch(1);

        listener.setForceSubscriptionAction(
                name -> {
                    if (!itemName.equals(name)) {
                        return;
                    }
                    activationEntered.countDown();
                    try {
                        boolean released = allowActivation.await(3, TimeUnit.SECONDS);
                        assertThat(released).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
                });

        AtomicReference<BufferedSubscribedItem> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        Thread getterThread =
                new Thread(
                        () -> {
                            try {
                                resultRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                errorRef.set(t);
                            }
                        },
                        "forceable-activation-race-getter");

        getterThread.start();
        assertThat(activationEntered.await(3, TimeUnit.SECONDS)).isTrue();

        assertThat(items.size()).isEqualTo(1);
        BufferedSubscribedItem placeholder =
                (BufferedSubscribedItem) items.values().iterator().next();

        Map<String, String> firstUpdate = Map.of("seq", "1");
        Map<String, String> secondUpdate = Map.of("seq", "2");

        placeholder.sendEvent(firstUpdate, listener, true);
        placeholder.clearSnapshot(listener);
        placeholder.endOfSnapshot(listener);
        placeholder.sendEvent(secondUpdate, listener, false);

        // While activation is blocked, events stay buffered and are not dispatched yet.
        assertThat(listener.getEvents()).isEmpty();

        allowActivation.countDown();
        getterThread.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(getterThread.isAlive()).isFalse();
        assertThat(errorRef.get()).isNull();

        BufferedSubscribedItem forced = resultRef.get();
        assertThat(forced).isNotNull();
        assertThat(forced.isForced()).isTrue();

        List<com.lightstreamer.kafka.test_utils.Mocks.EventCall> calls = listener.getEvents();
        assertThat(calls).hasSize(4);

        assertThat(calls.get(0).type()).isEqualTo(UPDATE);
        assertThat(calls.get(0).handle()).isEqualTo(itemHandle);
        assertThat(calls.get(0).event()).isEqualTo(firstUpdate);
        assertThat(calls.get(0).isSnapshot()).isTrue();

        assertThat(calls.get(1).type()).isEqualTo(CS);
        assertThat(calls.get(1).handle()).isEqualTo(itemHandle);

        assertThat(calls.get(2).type()).isEqualTo(EOS);
        assertThat(calls.get(2).handle()).isEqualTo(itemHandle);

        assertThat(calls.get(3).type()).isEqualTo(UPDATE);
        assertThat(calls.get(3).handle()).isEqualTo(itemHandle);
        assertThat(calls.get(3).event()).isEqualTo(secondUpdate);
        assertThat(calls.get(3).isSnapshot()).isFalse();
    }

    /**
     * Concurrent producer activity across activation transition preserves delivery: no event is
     * lost or duplicated while switching from queueing to direct dispatch.
     */
    @Test
    public void shouldCoverConcurrentActivationBoundaryWithoutLossOrDuplication() throws Exception {
        final String itemName = "boundary-race-[k=v]";
        final Object itemHandle = new Object();
        final int eventCount = 100;

        final CountDownLatch activationEntered = new CountDownLatch(1);
        final CountDownLatch allowActivation = new CountDownLatch(1);
        final CountDownLatch firstHalfProduced = new CountDownLatch(1);
        final CountDownLatch allowSecondHalf = new CountDownLatch(1);

        listener.setForceSubscriptionAction(
                name -> {
                    if (!itemName.equals(name)) {
                        return;
                    }
                    activationEntered.countDown();
                    try {
                        boolean released = allowActivation.await(3, TimeUnit.SECONDS);
                        assertThat(released).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
                });

        AtomicReference<BufferedSubscribedItem> getterResult = new AtomicReference<>();
        AtomicReference<Throwable> getterError = new AtomicReference<>();
        Thread getterThread =
                new Thread(
                        () -> {
                            try {
                                getterResult.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                getterError.set(t);
                            }
                        },
                        "forceable-boundary-race-getter");

        getterThread.start();
        assertThat(activationEntered.await(3, TimeUnit.SECONDS)).isTrue();

        assertThat(items.size()).isEqualTo(1);
        BufferedSubscribedItem placeholder =
                (BufferedSubscribedItem) items.values().iterator().next();

        AtomicReference<Throwable> producerError = new AtomicReference<>();
        Thread producerThread =
                new Thread(
                        () -> {
                            try {
                                for (int i = 0; i < eventCount; i++) {
                                    placeholder.sendEvent(
                                            Map.of("seq", Integer.toString(i)),
                                            listener,
                                            i < (eventCount / 2));
                                    if (i == (eventCount / 2) - 1) {
                                        firstHalfProduced.countDown();
                                        boolean released =
                                                allowSecondHalf.await(3, TimeUnit.SECONDS);
                                        assertThat(released).isTrue();
                                    }
                                }
                            } catch (Throwable t) {
                                producerError.set(t);
                            }
                        },
                        "forceable-boundary-race-producer");

        producerThread.start();
        assertThat(firstHalfProduced.await(3, TimeUnit.SECONDS)).isTrue();

        // Release activation and second-half production together to span the transition.
        allowActivation.countDown();
        allowSecondHalf.countDown();

        producerThread.join(TimeUnit.SECONDS.toMillis(3));
        getterThread.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(producerThread.isAlive()).isFalse();
        assertThat(getterThread.isAlive()).isFalse();
        assertThat(producerError.get()).isNull();
        assertThat(getterError.get()).isNull();

        BufferedSubscribedItem forced = getterResult.get();
        assertThat(forced).isNotNull();
        assertThat(forced.isForced()).isTrue();

        List<com.lightstreamer.kafka.test_utils.Mocks.EventCall> calls = listener.getEvents();
        assertThat(calls).hasSize(eventCount);
        for (int i = 0; i < eventCount; i++) {
            var call = calls.get(i);
            assertThat(call.type()).isEqualTo(UPDATE);
            assertThat(call.handle()).isEqualTo(itemHandle);
            assertThat(call.event()).isEqualTo(Map.of("seq", Integer.toString(i)));
            assertThat(call.isSnapshot()).isEqualTo(i < (eventCount / 2));
        }
    }

    /**
     * Deterministic race-lost scenario: both callers pass the slow path before activation, then
     * converge to the same forced entry once activation is released.
     */
    @Test
    public void shouldCoverConcurrentCase1BothCallersReachSlowPathBeforeActivation()
            throws Exception {
        final String itemName = "slow-path-race-[k=v]";
        final Object itemHandle = new Object();

        CountDownLatch callbackEntered = new CountDownLatch(2);
        CountDownLatch allowActivation = new CountDownLatch(1);
        AtomicInteger forceCalls = new AtomicInteger(0);

        listener.setForceSubscriptionAction(
                name -> {
                    if (!itemName.equals(name)) {
                        return;
                    }
                    forceCalls.incrementAndGet();
                    callbackEntered.countDown();
                    try {
                        boolean released = allowActivation.await(3, TimeUnit.SECONDS);
                        assertThat(released).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
                });

        AtomicReference<BufferedSubscribedItem> firstRef = new AtomicReference<>();
        AtomicReference<BufferedSubscribedItem> secondRef = new AtomicReference<>();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();
        CountDownLatch startGate = new CountDownLatch(1);

        Thread t1 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                firstRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                firstError.set(t);
                            }
                        },
                        "forceable-slow-path-race-1");

        Thread t2 =
                new Thread(
                        () -> {
                            try {
                                startGate.await();
                                secondRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                secondError.set(t);
                            }
                        },
                        "forceable-slow-path-race-2");

        t1.start();
        t2.start();
        startGate.countDown();

        assertThat(callbackEntered.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(items.size()).isEqualTo(1); // Single placeholder while activation is blocked.

        allowActivation.countDown();

        t1.join(TimeUnit.SECONDS.toMillis(3));
        t2.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(t1.isAlive()).isFalse();
        assertThat(t2.isAlive()).isFalse();
        assertThat(firstError.get()).isNull();
        assertThat(secondError.get()).isNull();

        BufferedSubscribedItem first = firstRef.get();
        BufferedSubscribedItem second = secondRef.get();
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first).isSameInstanceAs(second);
        assertThat(first.isForced()).isTrue();

        assertThat(items.size()).isEqualTo(1);
        assertThat(forceCalls.get()).isEqualTo(2);
    }

    /**
     * Mixed timing interleaving: while thread-1 is still inside forceSubscription after activation,
     * thread-2 should observe the forced entry via fast path and avoid a second forceSubscription
     * call.
     */
    @Test
    public void shouldCoverConcurrentCase0FastPathWhileFirstCallerUnwinds() throws Exception {
        final String itemName = "mixed-race-[k=v]";
        final Object itemHandle = new Object();

        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch allowActivation = new CountDownLatch(1);
        CountDownLatch activated = new CountDownLatch(1);
        CountDownLatch allowCallbackReturn = new CountDownLatch(1);
        AtomicInteger forceCalls = new AtomicInteger(0);

        listener.setForceSubscriptionAction(
                name -> {
                    if (!itemName.equals(name)) {
                        return;
                    }
                    forceCalls.incrementAndGet();
                    callbackEntered.countDown();
                    try {
                        assertThat(allowActivation.await(3, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    items.activateOrInstall(Expressions.Subscription(itemName), itemHandle);
                    activated.countDown();
                    try {
                        assertThat(allowCallbackReturn.await(3, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        AtomicReference<BufferedSubscribedItem> firstRef = new AtomicReference<>();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        Thread firstThread =
                new Thread(
                        () -> {
                            try {
                                firstRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                firstError.set(t);
                            }
                        },
                        "forceable-mixed-race-1");

        firstThread.start();
        assertThat(callbackEntered.await(3, TimeUnit.SECONDS)).isTrue();

        // Activate in callback, but keep first caller blocked before callback return.
        allowActivation.countDown();
        assertThat(activated.await(3, TimeUnit.SECONDS)).isTrue();

        AtomicReference<BufferedSubscribedItem> secondRef = new AtomicReference<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();
        Thread secondThread =
                new Thread(
                        () -> {
                            try {
                                secondRef.set(items.getItem(itemName));
                            } catch (Throwable t) {
                                secondError.set(t);
                            }
                        },
                        "forceable-mixed-race-2");

        secondThread.start();
        secondThread.join(TimeUnit.SECONDS.toMillis(3));
        assertThat(secondThread.isAlive()).isFalse();
        assertThat(secondError.get()).isNull();

        // Unblock callback and let first caller complete.
        allowCallbackReturn.countDown();
        firstThread.join(TimeUnit.SECONDS.toMillis(3));

        assertThat(firstThread.isAlive()).isFalse();
        assertThat(firstError.get()).isNull();

        BufferedSubscribedItem first = firstRef.get();
        BufferedSubscribedItem second = secondRef.get();
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first).isSameInstanceAs(second);
        assertThat(first.isForced()).isTrue();

        assertThat(items.size()).isEqualTo(1);
        assertThat(forceCalls.get()).isEqualTo(1);
    }

    // Concurrent test cases focus on getItem/activation races.
}
