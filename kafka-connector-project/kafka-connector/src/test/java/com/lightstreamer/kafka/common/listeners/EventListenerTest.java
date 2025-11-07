
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

package com.lightstreamer.kafka.common.listeners;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.test_utils.Mocks.RemoteTestEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.TestEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.UpdateCall;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class EventListenerTest {

    @Test
    public void shouldCreateSmartEventListener() {
        EventListener listener = EventListener.smartEventListener(new TestEventListener());
        assertThat(listener).isNotNull();
        assertThat(listener).isInstanceOf(SmartEventListener.class);
    }

    @Test
    public void shouldCreateLegacyEventListener() {
        EventListener listener = EventListener.legacyEventListener(new TestEventListener());
        assertThat(listener).isNotNull();
        assertThat(listener).isInstanceOf(LegacyEventListener.class);
    }

    @Test
    public void shouldCreateRemoteEventListener() {
        EventListener listener = EventListener.remoteEventListener(new RemoteTestEventListener());
        assertThat(listener).isNotNull();
        assertThat(listener).isInstanceOf(RemoteEventListener.class);
    }

    @Test
    public void shouldSendEventsToSmartListener() {
        TestEventListener itemEventListener = new TestEventListener();
        EventListener eventListener = EventListener.smartEventListener(itemEventListener);

        Map<String, String> updates = Map.of("field1", "value1", "field2", "value2");

        SubscribedItem item1 = Items.subscribedFrom("item1", new Object());
        eventListener.update(item1, updates, false);
        eventListener.update(item1, updates, true);
        assertThat(itemEventListener.getSmartRealtimeUpdateCount()).isEqualTo(1);

        List<UpdateCall> smartRealtimeUpdates = itemEventListener.getSmartRealtimeUpdates();
        UpdateCall call = smartRealtimeUpdates.get(0);
        assertThat(call.event()).isEqualTo(updates);
        assertThat(call.isSnapshot()).isFalse();
        assertThat(call.handle()).isEqualTo(item1.itemHandle());

        List<UpdateCall> smartSnapshotUpdates = itemEventListener.getSmartSnapshotUpdates();
        UpdateCall snapshotCall = smartSnapshotUpdates.get(0);
        assertThat(snapshotCall.event()).isEqualTo(updates);
        assertThat(snapshotCall.isSnapshot()).isTrue();
        assertThat(snapshotCall.handle()).isEqualTo(item1.itemHandle());

        SubscribedItem item2 = Items.subscribedFrom("item2", new Object());
        eventListener.clearSnapshot(item2);
        assertThat(itemEventListener.getSmartClearSnapshotCalls())
                .containsExactly(item2.itemHandle());

        SubscribedItem item3 = Items.subscribedFrom("item", new Object());
        eventListener.endOfSnapshot(item3);
        assertThat(itemEventListener.getSmartEndOfSnapshotCalls())
                .containsExactly(item3.itemHandle());

        eventListener.failure(new Exception("Test Exception"));
        assertThat(itemEventListener.getFailures()).hasSize(1);
        assertThat(itemEventListener.getFailures().get(0))
                .hasMessageThat()
                .isEqualTo("Test Exception");
    }

    @Test
    public void shouldSendEventsToLegacyListener() {
        TestEventListener itemEventListener = new TestEventListener();
        EventListener eventListener = EventListener.legacyEventListener(itemEventListener);

        Map<String, String> updates = Map.of("field1", "value1", "field2", "value2");

        SubscribedItem item1 = Items.subscribedFrom("item1");
        eventListener.update(item1, updates, false);
        eventListener.update(item1, updates, true);
        assertThat(itemEventListener.getRealtimeUpdateCount()).isEqualTo(1);

        List<UpdateCall> realtimeUpdates = itemEventListener.getRealtimeUpdates();
        UpdateCall call = realtimeUpdates.get(0);
        assertThat(call.event()).isEqualTo(updates);
        assertThat(call.isSnapshot()).isFalse();
        assertThat(call.handle()).isEqualTo(item1.itemHandle());

        List<UpdateCall> snapshotUpdates = itemEventListener.getSnapshotUpdates();
        UpdateCall snapshotCall = snapshotUpdates.get(0);
        assertThat(snapshotCall.event()).isEqualTo(updates);
        assertThat(snapshotCall.isSnapshot()).isTrue();
        assertThat(snapshotCall.handle()).isEqualTo(item1.itemHandle());

        SubscribedItem item2 = Items.subscribedFrom("item2");
        eventListener.clearSnapshot(item2);
        assertThat(itemEventListener.getClearSnapshotCalls()).containsExactly(item2.itemHandle());

        SubscribedItem item3 = Items.subscribedFrom("item3");
        eventListener.endOfSnapshot(item3);
        assertThat(itemEventListener.getEndOfSnapshotCalls()).containsExactly(item3.itemHandle());

        eventListener.failure(new Exception("Test Exception"));
        assertThat(itemEventListener.getFailures()).hasSize(1);
        assertThat(itemEventListener.getFailures().get(0))
                .hasMessageThat()
                .isEqualTo("Test Exception");
    }

    @Test
    public void shouldSendEventsToRemoteListener() {
        RemoteTestEventListener itemEventListener = new RemoteTestEventListener();
        EventListener eventListener = EventListener.remoteEventListener(itemEventListener);

        Map<String, String> updates = Map.of("field1", "value1", "field2", "value2");

        SubscribedItem item1 = Items.subscribedFrom("item1");
        eventListener.update(item1, updates, false);
        eventListener.update(item1, updates, true);
        assertThat(itemEventListener.getRealtimeUpdateCount()).isEqualTo(1);

        List<UpdateCall> realtimeUpdates = itemEventListener.getRealtimeUpdates();
        UpdateCall call = realtimeUpdates.get(0);
        assertThat(call.event()).isEqualTo(updates);
        assertThat(call.isSnapshot()).isFalse();
        assertThat(call.handle()).isEqualTo(item1.itemHandle());

        List<UpdateCall> snapshotUpdates = itemEventListener.getSnapshotUpdates();
        UpdateCall snapshotCall = snapshotUpdates.get(0);
        assertThat(snapshotCall.event()).isEqualTo(updates);
        assertThat(snapshotCall.isSnapshot()).isTrue();
        assertThat(snapshotCall.handle()).isEqualTo(item1.itemHandle());

        SubscribedItem item2 = Items.subscribedFrom("item2");
        eventListener.clearSnapshot(item2);
        assertThat(itemEventListener.getClearSnapshotCalls()).containsExactly(item2.itemHandle());

        SubscribedItem item3 = Items.subscribedFrom("item3");
        eventListener.endOfSnapshot(item3);
        assertThat(itemEventListener.getEndOfSnapshotCalls()).containsExactly(item3.itemHandle());

        eventListener.failure(new Exception("Test Exception"));
        assertThat(itemEventListener.getFailures()).hasSize(1);
        assertThat(itemEventListener.getFailures().get(0))
                .hasMessageThat()
                .isEqualTo("Test Exception");
    }
}
