
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

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultSubscribedItemTest {

    /** Test double for ItemEventListener that records all method calls for verification */
    private static class TestEventListener implements EventListener {

        private final List<UpdateCall> snapshotUpdates =
                Collections.synchronizedList(new ArrayList<>());
        private final List<UpdateCall> realtimeUpdates =
                Collections.synchronizedList(new ArrayList<>());
        private final List<UpdateCall> allUpdatesChronological =
                Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribedItem> clearSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribedItem> endOfSnapshotCalls =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void update(SubscribedItem item, Map<String, String> event, boolean isSnapshot) {
            Map<String, String> stringMap = (Map<String, String>) event;
            UpdateCall call = new UpdateCall(item, stringMap, isSnapshot);

            // Add to both category-specific and chronological lists
            allUpdatesChronological.add(call);
            if (isSnapshot) {
                snapshotUpdates.add(call);
            } else {
                realtimeUpdates.add(call);
            }
        }

        @Override
        public void clearSnapshot(SubscribedItem item) {
            clearSnapshotCalls.add(item);
        }

        @Override
        public void endOfSnapshot(SubscribedItem item) {
            endOfSnapshotCalls.add(item);
        }

        @Override
        public void failure(Exception exception) {
            // Not used in tests
        }

        public List<UpdateCall> getAllUpdatesChronological() {
            return new ArrayList<>(allUpdatesChronological);
        }

        public List<UpdateCall> getSnapshotUpdates() {
            return new ArrayList<>(snapshotUpdates);
        }

        public List<UpdateCall> getRealtimeUpdates() {
            return new ArrayList<>(realtimeUpdates);
        }

        public int getRealtimeUpdateCount() {
            return realtimeUpdates.size();
        }
    }

    private static class UpdateCall {
        final SubscribedItem item;
        final Map<String, String> event;
        final boolean isSnapshot;

        UpdateCall(SubscribedItem item, Map<String, String> event, boolean isSnapshot) {
            this.item = item;
            this.event = Map.copyOf(event); // Defensive copy
            this.isSnapshot = isSnapshot;
        }

        @Override
        public String toString() {
            return String.format(
                    "UpdateCall{handle=%s, event=%s, isSnapshot=%s}",
                    item.asCanonicalItemName(), event, isSnapshot);
        }
    }

    private TestEventListener testListener;
    private SubscribedItem subscribedItem;
    private final Object itemHandle = "test-item-handle";

    @BeforeEach
    void setUp() throws Exception {
        testListener = new TestEventListener();
        subscribedItem = Items.subscribedFrom(Expressions.Subscription("test-item"), itemHandle);
    }

    @Test
    void shouldHandleRealtimeAndSnapshotEventsCorrectly() {
        // Phase 1: Send mixed events before snapshot processing
        Map<String, String> snapshotEvent1 = Map.of("field1", "snapshot1");
        Map<String, String> queuedEvent1 = Map.of("field1", "queued1");
        Map<String, String> snapshotEvent2 = Map.of("field1", "snapshot2");
        Map<String, String> queuedEvent2 = Map.of("field1", "queued2");

        // Snapshot events should go directly to listener regardless of state
        subscribedItem.sendSnapshotEvent(snapshotEvent1, testListener);
        subscribedItem.sendRealtimeEvent(queuedEvent1, testListener);
        subscribedItem.sendSnapshotEvent(snapshotEvent2, testListener);
        subscribedItem.sendRealtimeEvent(queuedEvent2, testListener);

        // Verify snapshot events were processed immediately, realtime events queued
        List<UpdateCall> snapshotUpdates = testListener.getSnapshotUpdates();
        assertThat(snapshotUpdates).hasSize(2);
        assertThat(snapshotUpdates.get(0).event).isEqualTo(snapshotEvent1);
        assertThat(snapshotUpdates.get(1).event).isEqualTo(snapshotEvent2);
        assertThat(testListener.getRealtimeUpdateCount()).isEqualTo(0); // Queued, not processed

        // Phase 2: Process snapshot - should flush queued realtime events
        subscribedItem.snapshotProcessed(testListener);
        assertThat(testListener.getRealtimeUpdateCount()).isEqualTo(2);

        // Phase 3: Send mixed events after snapshot processing - all should go directly
        Map<String, String> directEvent1 = Map.of("field1", "direct1");
        Map<String, String> snapshotEvent3 = Map.of("field1", "snapshot3");
        Map<String, String> directEvent2 = Map.of("field1", "direct2");
        Map<String, String> snapshotEvent4 = Map.of("field1", "snapshot4");

        subscribedItem.sendRealtimeEvent(directEvent1, testListener);
        subscribedItem.sendSnapshotEvent(snapshotEvent3, testListener);
        subscribedItem.sendRealtimeEvent(directEvent2, testListener);
        subscribedItem.sendSnapshotEvent(snapshotEvent4, testListener);

        // Verify final state - all events processed in correct categories
        List<UpdateCall> finalSnapshotUpdates = testListener.getSnapshotUpdates();
        assertThat(finalSnapshotUpdates).hasSize(4);
        assertThat(finalSnapshotUpdates.get(2).event).isEqualTo(snapshotEvent3);
        assertThat(finalSnapshotUpdates.get(3).event).isEqualTo(snapshotEvent4);

        List<UpdateCall> realtimeUpdates = testListener.getRealtimeUpdates();
        assertThat(realtimeUpdates).hasSize(4);
        assertThat(realtimeUpdates.get(0).event).isEqualTo(queuedEvent1);
        assertThat(realtimeUpdates.get(1).event).isEqualTo(queuedEvent2);
        assertThat(realtimeUpdates.get(2).event).isEqualTo(directEvent1);
        assertThat(realtimeUpdates.get(3).event).isEqualTo(directEvent2);

        // Verify correct event marking
        for (UpdateCall call : finalSnapshotUpdates) {
            assertThat(call.isSnapshot).isTrue();
        }
        for (UpdateCall call : realtimeUpdates) {
            assertThat(call.isSnapshot).isFalse();
        }
    }

    @Test
    @Timeout(10)
    void shouldHandleConcurrentEventsAndTransition() throws Exception {
        final AtomicInteger threadCounter = new AtomicInteger(0);
        final ExecutorService executor =
                Executors.newFixedThreadPool(
                        15,
                        r -> {
                            Thread t = new Thread(r);
                            t.setDaemon(true);
                            t.setName("thread-" + threadCounter.incrementAndGet());
                            return t;
                        });
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch snapshotDoneLatch = new CountDownLatch(1);
        final AtomicInteger realtimeEventCounter = new AtomicInteger(0);

        String snapshotEventPrefix = "A.";
        String realtimeEventPrefix = "B.";

        final Set<String> threads = new TreeSet<>();

        // Threads 1-14: Send concurrent real-time events
        List<CompletableFuture<Void>> realTime = new ArrayList<>();
        for (int i = 0; i < 14; i++) {
            realTime.add(
                    CompletableFuture.runAsync(
                            () -> {
                                threads.add(Thread.currentThread().getName());
                                try {
                                    startLatch.await();
                                    for (int j = 0; j < 20; j++) {
                                        realtimeEventCounter.incrementAndGet();
                                        subscribedItem.sendRealtimeEvent(
                                                Map.of(
                                                        "id",
                                                        realtimeEventPrefix
                                                                + realtimeEventCounter.get()),
                                                null);
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            executor));
        }

        // Thread 5: Process snapshot and send post-transition events
        Runnable runnable =
                () -> {
                    try {
                        startLatch.await();

                        // Send some snapshot events during the transition (before
                        // snapshotProcessed)
                        // These should be processed immediately, not queued
                        subscribedItem.sendSnapshotEvent(
                                Map.of("id", snapshotEventPrefix + "1"), testListener);
                        subscribedItem.sendSnapshotEvent(
                                Map.of("id", snapshotEventPrefix + "2"), testListener);

                        Thread.sleep(10); // Let some real-time events accumulate
                        subscribedItem.snapshotProcessed(testListener);
                        snapshotDoneLatch.countDown();

                        // Send post-transition events (both realtime and snapshot)
                        for (int i = 1; i <= 3; i++) {
                            // subscribedItem.sendRealtimeEvent(Map.of("id", "post" + i));
                            // subscribedItem.sendSnapshotEvent(
                            //         Map.of("snapshot", "after" + i));
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };
        CompletableFuture<Void> snapshot = CompletableFuture.runAsync(runnable, executor);

        // Start all threads
        startLatch.countDown();

        // Wait for completion with generous timeout
        for (CompletableFuture<Void> future : realTime) {
            future.join();
        }
        snapshot.join();

        executor.shutdown();
        assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        assertThat(threads).hasSize(14); // All threads ran

        // Verify all events were processed exactly once
        int expectedTotalRealtimeEvents = 14 * 20; // 14 threads * 20 events each = 280
        int expectedSnapshotEvents = 2;
        int expectedTotalEvents = expectedTotalRealtimeEvents + expectedSnapshotEvents;

        assertThat(realtimeEventCounter.get())
                .isEqualTo(expectedTotalRealtimeEvents); // Only stress events are counted
        assertThat(testListener.getRealtimeUpdateCount()).isEqualTo(expectedTotalRealtimeEvents);
        assertThat(testListener.getSnapshotUpdates()).hasSize(expectedSnapshotEvents);
        assertThat(
                        testListener.getRealtimeUpdates().stream()
                                .filter(c -> c.event.get("id").startsWith(realtimeEventPrefix))
                                .count())
                .isEqualTo(expectedTotalRealtimeEvents);
        List<UpdateCall> allUpdatesChronological = testListener.getAllUpdatesChronological();
        assertThat(testListener.getAllUpdatesChronological()).hasSize(expectedTotalEvents);

        // Verify that snapshot events are delivered before real-time events
        List<String> orderedEvents =
                allUpdatesChronological.stream()
                        .map(
                                call -> {
                                    String value = call.event.get("id");
                                    if (value.startsWith(realtimeEventPrefix)) {
                                        return realtimeEventPrefix + "real-time event";
                                    } else if (value.startsWith(snapshotEventPrefix)) {
                                        return snapshotEventPrefix + "snapshot event";
                                    } else {
                                        return "unknown";
                                    }
                                })
                        .distinct()
                        .toList();
        assertThat(orderedEvents)
                .containsExactly("A.snapshot event", "B.real-time event")
                .inOrder();
    }

    @Test
    void shouldHandleMultipleSnapshotProcessedCalls() {
        // Send some events first
        Map<String, String> event1 = Map.of("field1", "value1");
        subscribedItem.sendRealtimeEvent(event1, null);

        // Process snapshot multiple times (should be idempotent after first call)
        subscribedItem.snapshotProcessed(testListener);

        int eventsAfterFirstCall = testListener.getRealtimeUpdateCount();

        subscribedItem.snapshotProcessed(testListener);

        // Send event after
        Map<String, String> event2 = Map.of("field1", "value2");
        subscribedItem.sendRealtimeEvent(event2, testListener);

        subscribedItem.snapshotProcessed(testListener);

        // Should not process any additional events
        assertThat(testListener.getRealtimeUpdateCount()).isEqualTo(eventsAfterFirstCall);

        // Verify events were processed correctly
        List<UpdateCall> realtimeUpdates = testListener.getRealtimeUpdates();
        assertThat(realtimeUpdates).hasSize(2);
        assertThat(realtimeUpdates.get(0).event).isEqualTo(event1);
        assertThat(realtimeUpdates.get(1).event).isEqualTo(event2);
    }

    @Test
    void shouldClearSnapshot() {
        // Send some snapshot events
        Map<String, String> snapshotEvent1 = Map.of("field1", "snapshot1");
        Map<String, String> snapshotEvent2 = Map.of("field1", "snapshot2");

        subscribedItem.sendSnapshotEvent(snapshotEvent1, testListener);
        subscribedItem.sendSnapshotEvent(snapshotEvent2, testListener);

        // Clear snapshot
        subscribedItem.clearSnapshot(testListener);

        // Verify clearSnapshot was called
        assertThat(testListener.clearSnapshotCalls).hasSize(1);
        assertThat(testListener.clearSnapshotCalls.get(0)).isEqualTo(subscribedItem);

        // Send another snapshot event after clearing
        Map<String, String> snapshotEvent3 = Map.of("field1", "snapshot3");
        subscribedItem.sendSnapshotEvent(snapshotEvent3, testListener);

        // Verify all snapshot events were processed correctly
        List<UpdateCall> snapshotUpdates = testListener.getSnapshotUpdates();
        assertThat(snapshotUpdates).hasSize(3);
        assertThat(snapshotUpdates.get(0).event).isEqualTo(snapshotEvent1);
        assertThat(snapshotUpdates.get(1).event).isEqualTo(snapshotEvent2);
        assertThat(snapshotUpdates.get(2).event).isEqualTo(snapshotEvent3);
    }

    @Test
    void shouldEndSnapshot() {
        // End snapshot
        subscribedItem.endOfSnapshot(testListener);

        // Verify endOfSnapshot was called
        assertThat(testListener.endOfSnapshotCalls).hasSize(1);
        assertThat(testListener.endOfSnapshotCalls).containsExactly(subscribedItem);
    }

    @Test
    void shouldMaintainSnapshotFlagBehavior() {
        // Initially in snapshot mode
        assertThat(subscribedItem.isSnapshot()).isTrue();

        // Change flag
        subscribedItem.setSnapshot(false);
        assertThat(subscribedItem.isSnapshot()).isFalse();

        // Change back
        subscribedItem.setSnapshot(true);
        assertThat(subscribedItem.isSnapshot()).isTrue();
    }
}
