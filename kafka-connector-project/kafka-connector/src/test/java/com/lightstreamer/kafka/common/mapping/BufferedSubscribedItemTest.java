
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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.Items.BufferedSubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.test_utils.Mocks.EventCall;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import java.util.stream.Stream;

public class BufferedSubscribedItemTest {

    private MockItemEventListener eventListener;
    private BufferedSubscribedItem subscribedItem;

    @BeforeEach
    public void setUp() throws Exception {
        this.eventListener = new MockItemEventListener();
        this.subscribedItem =
                new BufferedSubscribedItem(Expressions.Subscription("item-[name=field1]"));
    }

    @Test
    public void shouldCreateBufferedSubscribedItemFromFactory() {
        String expression = "item-[name=field1]";
        BufferedSubscribedItem item = Items.bufferedSubscribedFrom(expression);
        assertThat(item).isNotNull();
        assertThat(item.schema().name()).isEqualTo("item");
        assertThat(item.schema().keys()).containsExactly("name");
        assertThat(item.canonicalName()).isEqualTo("item-[name=field1]");
    }

    static Stream<Arguments> provideExpressions() {
        return Stream.of(
                arguments("item", "item", Collections.emptySet(), "item"),
                arguments("item-first", "item-first", Collections.emptySet(), "item-first"),
                arguments("item_123_", "item_123_", Collections.emptySet(), "item_123_"),
                arguments("item-", "item-", Collections.emptySet(), "item-"),
                arguments("prefix-[]", "prefix", Collections.emptySet(), "prefix"),
                arguments("item-[name=field1]", "item", Set.of("name"), "item-[name=field1]"),
                arguments(
                        "item-[name2=field2,name1=field1]",
                        "item",
                        Set.of("name2", "name1"),
                        "item-[name1=field1,name2=field2]"),
                arguments(
                        "item-first-[height=12.34]",
                        "item-first",
                        Set.of("height"),
                        "item-first-[height=12.34]"),
                arguments(
                        "item_123_-[test=\\]", "item_123_", Set.of("test"), "item_123_-[test=\\]"),
                arguments("item-[test=\"\"]", "item", Set.of("test"), "item-[test=\"\"]"),
                arguments("prefix-[test=]]", "prefix", Set.of("test"), "prefix-[test=]]"),
                arguments("item-[test=value,]", "item", Set.of("test"), "item-[test=value]"));
    }

    @ParameterizedTest
    @MethodSource("provideExpressions")
    public void shouldCreateBufferedSubscribedItem(
            String expression,
            String expectedPrefix,
            Set<String> expectedKeys,
            String expectedCanonicalItemName) {
        BufferedSubscribedItem item =
                new BufferedSubscribedItem(Expressions.Subscription(expression));
        assertThat(item).isNotNull();
        assertThat(item.schema().name()).isEqualTo(expectedPrefix);
        assertThat(item.schema().keys()).isEqualTo(expectedKeys);
        assertThat(item.canonicalName()).isEqualTo(expectedCanonicalItemName);
        assertThat(item.equals(item)).isTrue();
    }

    @Test
    public void shouldAccumulatePendingEventsUntilEnableEventsDelivery() {
        // Phase 1: Send mixed events before unlocking. ALL events (snapshot or real-time) are
        // buffered and must be drained in insertion order, each preserving its original flag.
        Map<String, String> snapshotEvent1 = Map.of("field1", "snapshot1");
        Map<String, String> realTimeEvent1 = Map.of("field1", "realTime1");
        Map<String, String> snapshotEvent2 = Map.of("field1", "snapshot2");
        Map<String, String> realTimeEvent2 = Map.of("field1", "realTime2");

        subscribedItem.sendEvent(snapshotEvent1, eventListener, true);
        subscribedItem.clearSnapshot(eventListener);
        subscribedItem.sendEvent(realTimeEvent1, eventListener, false);
        subscribedItem.sendEvent(snapshotEvent2, eventListener, true);
        subscribedItem.endOfSnapshot(eventListener);
        subscribedItem.sendEvent(realTimeEvent2, eventListener, false);

        // Nothing has been delivered yet.
        assertThat(eventListener.getEvents()).isEmpty();

        // Phase 2: Unlock delivery — the buffered events drain in insertion order, each with its
        // original isSnapshot flag.
        Object itemHandle = new Object();
        subscribedItem.enableEventsDelivery(itemHandle, eventListener);

        List<EventCall> allEvents = eventListener.getEvents();
        assertThat(allEvents).hasSize(6);
        EventCall eventCall = allEvents.get(0);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(snapshotEvent1);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(1);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.CS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(2);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.event()).isEqualTo(realTimeEvent1);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.isSnapshot()).isFalse();

        eventCall = allEvents.get(3);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.event()).isEqualTo(snapshotEvent2);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(4);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.EOS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(5);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.event()).isEqualTo(realTimeEvent2);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.isSnapshot()).isFalse();

        // Clear the listener for the next phases.
        eventListener.reset();

        // Phase 3: After unlocking, events are delivered immediately without buffering, each with
        // its original isSnapshot flag.
        Map<String, String> realTimeEvent3 = Map.of("field1", "direct1");
        Map<String, String> snapshotEvent3 = Map.of("field1", "snapshot3");
        Map<String, String> realTimeEvent4 = Map.of("field1", "direct2");
        Map<String, String> snapshotEvent4 = Map.of("field1", "snapshot4");

        subscribedItem.sendEvent(realTimeEvent3, eventListener, false);
        subscribedItem.sendEvent(snapshotEvent3, eventListener, true);
        subscribedItem.endOfSnapshot(eventListener);
        subscribedItem.sendEvent(realTimeEvent4, eventListener, false);
        subscribedItem.sendEvent(snapshotEvent4, eventListener, true);
        subscribedItem.clearSnapshot(eventListener);

        allEvents = eventListener.getEvents();
        assertThat(allEvents).hasSize(6);

        eventCall = allEvents.get(0);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.event()).isEqualTo(realTimeEvent3);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.isSnapshot()).isFalse();

        eventCall = allEvents.get(1);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(snapshotEvent3);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(2);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.EOS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(3);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(realTimeEvent4);
        assertThat(eventCall.isSnapshot()).isFalse();

        eventCall = allEvents.get(4);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(snapshotEvent4);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(5);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.CS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
    }

    @Test
    @Timeout(10)
    public void shouldHandleConcurrentEventsAndTransition() throws Exception {
        final AtomicInteger threadCounter = new AtomicInteger(0);
        final ExecutorService executor =
                Executors.newFixedThreadPool(
                        10,
                        r -> {
                            Thread t = new Thread(r);
                            t.setDaemon(true);
                            t.setName("thread-" + threadCounter.incrementAndGet());
                            return t;
                        });
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger realtimeEventCounter = new AtomicInteger(0);

        String snapshotEventPrefix = "A.";
        String realtimeEventPrefix = "B.";

        final Set<String> threads = Collections.synchronizedSet(new TreeSet<>());

        // Threads 1-5: send concurrent real-time events.
        List<CompletableFuture<Void>> realTime = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            realTime.add(
                    CompletableFuture.runAsync(
                            () -> {
                                threads.add(Thread.currentThread().getName());
                                try {
                                    startLatch.await();
                                    for (int j = 0; j < 20; j++) {
                                        realtimeEventCounter.incrementAndGet();
                                        TimeUnit.MILLISECONDS.sleep((long) (Math.random() * 15));
                                        subscribedItem.sendEvent(
                                                Map.of(
                                                        "id",
                                                        realtimeEventPrefix
                                                                + realtimeEventCounter.get()),
                                                eventListener,
                                                false);
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            executor));
        }

        // Thread 6: buffer mixed events then activate and send post-transition events.
        CompletableFuture<Void> activator =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                startLatch.await();

                                // Buffer snapshot events, a clearSnapshot, and an endOfSnapshot
                                // before enableEventsDelivery. These exercise all PendingEvent
                                // types in drainTo.
                                subscribedItem.sendEvent(
                                        Map.of("id", snapshotEventPrefix + "1"),
                                        eventListener,
                                        true);
                                subscribedItem.clearSnapshot(eventListener);
                                subscribedItem.sendEvent(
                                        Map.of("id", snapshotEventPrefix + "2"),
                                        eventListener,
                                        true);
                                subscribedItem.endOfSnapshot(eventListener);

                                Thread.sleep(5); // Let some real-time events accumulate.
                                subscribedItem.enableEventsDelivery(new Object(), eventListener);

                                // Send post-transition events (both realtime and snapshot).
                                for (int i = 1; i <= 3; i++) {
                                    subscribedItem.sendEvent(
                                            Map.of("id", "post" + i), eventListener, false);
                                    subscribedItem.sendEvent(
                                            Map.of("snapshot", "after" + i), eventListener, true);
                                    Thread.sleep(1);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        executor);

        // Thread 7: send clearSnapshot() calls with short random delays, exercising the
        // redirect path when activation swaps the dispatcher mid-flight.
        int clearSnapshotCount = 30;
        CompletableFuture<Void> csFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                startLatch.await();
                                for (int i = 0; i < clearSnapshotCount; i++) {
                                    TimeUnit.MILLISECONDS.sleep((long) (Math.random() * 5));
                                    subscribedItem.clearSnapshot(eventListener);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        executor);

        // Threads 8-10: send endOfSnapshot() calls with short random delays from multiple
        // threads, exercising the redirect path when activation swaps the dispatcher mid-flight.
        int eosThreadCount = 3;
        int eosCallsPerThread = 30;
        int endOfSnapshotCount = eosThreadCount * eosCallsPerThread;
        List<CompletableFuture<Void>> eosFutures = new ArrayList<>();
        for (int t = 0; t < eosThreadCount; t++) {
            eosFutures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int i = 0; i < eosCallsPerThread; i++) {
                                        TimeUnit.MILLISECONDS.sleep((long) (Math.random() * 5));
                                        subscribedItem.endOfSnapshot(eventListener);
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            executor));
        }

        // Start all threads.
        startLatch.countDown();

        // Wait for completion.
        for (CompletableFuture<Void> future : realTime) {
            future.join();
        }
        activator.join();
        csFuture.join();
        for (CompletableFuture<Void> future : eosFutures) {
            future.join();
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threads).hasSize(5);

        // Verify all events were processed exactly once. Every event (snapshot or real-time)
        // sent before enableEventsDelivery is buffered and drained in insertion order, so the
        // two streams interleave. We only assert the totals here.
        int expectedStressRealtimeEvents = 5 * 20; // 5 threads * 20 events each = 100
        int expectedPostTransitionRealtime = 3; // "post1", "post2", "post3"
        int expectedSnapshotEvents = 2 + 3; // 2 pre-transition + 3 post-transition
        // The activator explicitly buffers 1 CS + 1 EOS before activation.
        int bufferedClearSnapshots = 1;
        int bufferedEndOfSnapshots = 1;
        int expectedTotalRealtime = expectedStressRealtimeEvents + expectedPostTransitionRealtime;
        int expectedTotalUpdates = expectedTotalRealtime + expectedSnapshotEvents;
        int expectedTotalEvents =
                expectedTotalUpdates
                        + (clearSnapshotCount + bufferedClearSnapshots)
                        + (endOfSnapshotCount + bufferedEndOfSnapshots);

        assertThat(realtimeEventCounter.get())
                .isEqualTo(expectedStressRealtimeEvents); // Only stress events are counted.
        assertThat(eventListener.getSmartRealtimeUpdates()).hasSize(expectedTotalRealtime);
        assertThat(eventListener.getSmartSnapshotUpdates()).hasSize(expectedSnapshotEvents);
        assertThat(
                        eventListener.getSmartRealtimeUpdates().stream()
                                .filter(c -> c.event().get("id").startsWith(realtimeEventPrefix))
                                .count())
                .isEqualTo(expectedStressRealtimeEvents);
        assertThat(eventListener.getSmartClearSnapshotCalls())
                .hasSize(clearSnapshotCount + bufferedClearSnapshots);
        assertThat(eventListener.getSmartEndOfSnapshotCalls())
                .hasSize(endOfSnapshotCount + bufferedEndOfSnapshots);
        assertThat(eventListener.getEvents()).hasSize(expectedTotalEvents);
    }

    @Test
    public void shouldEnableEventsDeliveryBeIdempotent() {
        // Send some events first.
        Map<String, String> event1 = Map.of("field1", "value1");
        subscribedItem.sendEvent(event1, eventListener, false);

        Object itemHandle = new Object();

        // Trigger enableEventsDelivery.
        subscribedItem.enableEventsDelivery(itemHandle, eventListener);
        assertThat(eventListener.getSmartRealtimeUpdates()).hasSize(1);

        // Second call should have no effect.
        subscribedItem.enableEventsDelivery(itemHandle, eventListener);
        assertThat(eventListener.getSmartRealtimeUpdates()).hasSize(1);

        // Send event after.
        Map<String, String> event2 = Map.of("field1", "value2");
        subscribedItem.sendEvent(event2, eventListener, false);

        // Verify events were processed correctly.
        List<EventCall> realtimeUpdates = eventListener.getSmartRealtimeUpdates();
        assertThat(realtimeUpdates).hasSize(2);
        assertThat(realtimeUpdates.get(0).event()).isEqualTo(event1);
        assertThat(realtimeUpdates.get(1).event()).isEqualTo(event2);
    }

    @Test
    public void shouldMarkForced() {
        // Mark the item as forced.
        subscribedItem.markForced();

        // Verify that the item is marked as forced.
        assertThat(subscribedItem.isForced()).isTrue();
    }

    @Test
    public void shouldMaintainSnapshotFlagBehavior() {
        // Initially in snapshot mode.
        assertThat(subscribedItem.isSnapshot()).isTrue();

        // Change flag.
        subscribedItem.setSnapshot(false);
        assertThat(subscribedItem.isSnapshot()).isFalse();

        // Change back.
        subscribedItem.setSnapshot(true);
        assertThat(subscribedItem.isSnapshot()).isTrue();
    }
}
