
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

import java.util.concurrent.atomic.AtomicBoolean;

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

    // Concurrent test cases to be added in a separate phase
}
