
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.EventUpdater;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.LegacyEventUpdater;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.SmartEventUpdater;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class EventUpdaterTest {

    @Test
    public void shouldCreateSmartUpdater() {
        EventUpdater eventUpdater = EventUpdater.create(new MockItemEventListener(), false);
        assertThat(eventUpdater).isNotNull();
        assertThat(eventUpdater).isInstanceOf(SmartEventUpdater.class);
    }

    @Test
    public void shouldCreateLegacyUpdater() {
        EventUpdater eventUpdater = EventUpdater.create(new MockItemEventListener(), true);
        assertThat(eventUpdater).isNotNull();
        assertThat(eventUpdater).isInstanceOf(LegacyEventUpdater.class);
    }

    @Test
    public void shouldForwardEventsFromSmartUpdater() {
        AtomicInteger counter = new AtomicInteger(0);
        BiConsumer<Map<String, String>, Boolean> smartConsumer =
                (update, isSnapshot) -> {
                    counter.incrementAndGet();
                };
        MockItemEventListener listener = new MockItemEventListener(smartConsumer);
        EventUpdater eventUpdater = EventUpdater.create(listener, false);
        HashMap<String, String> updates = new HashMap<>();
        eventUpdater.update(Items.subscribedFrom("item"), updates, false);

        assertThat(counter.get()).isEqualTo(1);

        eventUpdater.clearSnapshot(Items.subscribedFrom("item"));
        assertThat(listener.smartClearSnapshotCalled()).isTrue();
        assertThat(listener.legacyClearSnapshotCalled()).isFalse();

        eventUpdater.endOfSnapshot(Items.subscribedFrom("item"));
        assertThat(listener.smartEndOfSnapshotCalled()).isTrue();
        assertThat(listener.legacyEndOfSnapshotCalled()).isFalse();
    }

    @Test
    public void shouldForwardEventsFromLegacyUpdater() {
        AtomicInteger counter = new AtomicInteger(0);
        BiConsumer<Map<String, String>, Boolean> legacyConsumer =
                (event, isSnapshot) -> {
                    counter.incrementAndGet();
                };
        BiConsumer<Map<String, String>, Boolean> smartConsumer = MockItemEventListener.NOPConsumer;
        MockItemEventListener listener = new MockItemEventListener(smartConsumer, legacyConsumer);
        EventUpdater eventUpdater = EventUpdater.create(listener, true);
        HashMap<String, String> event = new HashMap<>();
        eventUpdater.update(Items.subscribedFrom("item"), event, false);

        assertThat(counter.get()).isEqualTo(1);

        eventUpdater.clearSnapshot(Items.subscribedFrom("item"));
        assertThat(listener.legacyClearSnapshotCalled()).isTrue();
        assertThat(listener.smartClearSnapshotCalled()).isFalse();

        eventUpdater.endOfSnapshot(Items.subscribedFrom("item"));
        assertThat(listener.legacyEndOfSnapshotCalled()).isTrue();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();
    }
}
