
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.EventUpdater;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RouteAllStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class RecordProcessorTest {

    static class EventConsumer implements BiConsumer<Map<String, String>, Boolean> {

        private AtomicInteger counter = new AtomicInteger();
        private AtomicBoolean snapshotEvent = new AtomicBoolean();
        private Map<String, String> lastUpdates;

        @Override
        public void accept(Map<String, String> updates, Boolean isSnapshot) {
            this.lastUpdates = updates;
            counter.incrementAndGet();
            snapshotEvent.set(isSnapshot);
        }

        int getCounter() {
            return counter.get();
        }

        void resetCounter() {
            counter.set(0);
        }

        boolean isSnapshotEvent() {
            return snapshotEvent.get();
        }

        void resetSnapshotEvent() {
            snapshotEvent.set(false);
        }

        Map<String, String> getLastUpdates() {
            return lastUpdates;
        }
    }

    private static final String TEST_TOPIC = "topic";

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    private RecordMapper<String, String> mapper;

    private EventConsumer smartConsumer = new EventConsumer();
    private EventConsumer legacyConsumer = new EventConsumer();
    private MockItemEventListener listener;

    private ConsumerRecord<String, String> record;
    private ConsumerRecord<String, String> recordWithNullPayload;
    private Set<SubscribedItem> subscribedItems;
    private RecordProcessor<String, String> processor;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                extractor(String(), "item1", Collections.emptyMap(), false, false))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                extractor(String(), "item2", Collections.emptyMap(), false, false))
                        .withFieldExtractor(
                                extractor(String(), "field1", Collections.emptyMap(), false, false))
                        .build();

        // The mocked ItemEventListener instance, which updates the counter upon invocation
        this.listener = new MockItemEventListener(smartConsumer, legacyConsumer);

        // A record routable to "item1" and "item2"
        this.record = Records.ConsumerRecord(TEST_TOPIC, 0, "a-1");

        // A record with a null payload, which should be processed as DELETE event
        this.recordWithNullPayload = Records.ConsumerRecord(TEST_TOPIC, "aKey", null);

        // The collection of subscribable items
        this.subscribedItems = new HashSet<>();
    }

    RecordProcessor<String, String> processor(
            SubscribedItems subscribedItems, ProcessUpdatesStrategy updatesStrategy) {
        return new DefaultRecordProcessor<>(
                mapper,
                EventUpdater.create(listener, subscribedItems.isNop()),
                updatesStrategy,
                subscribedItems.isNop()
                        ? new RouteAllStrategy()
                        : new DefaultRoutingStrategy(subscribedItems));
    }

    @Test
    public void shouldProcess() {
        processor =
                processor(
                        SubscribedItems.of(subscribedItems),
                        ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to "item1" and process the record
        subscribedItems.add(Items.subscribedFrom("item1", new Object()));
        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isFalse();

        // Reset the counter
        smartConsumer.resetCounter();
        // Reset the snapshot flag
        smartConsumer.resetSnapshotEvent();

        // Add subscription "item2" and process the record
        subscribedItems.add(Items.subscribedFrom("item2", new Object()));
        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(smartConsumer.getCounter()).isEqualTo(2);
    }

    @Test
    public void shouldProcessWithNoSubscriptions() {
        processor = processor(SubscribedItems.nop(), ProcessUpdatesStrategy.defaultStrategy());
        processor.process(record);

        // Verify that the update has been routed for all the available
        // item templates
        assertThat(legacyConsumer.getCounter()).isEqualTo(2);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(legacyConsumer.isSnapshotEvent()).isFalse();
    }

    @Test
    public void shouldNotProcessUnexpectedSubscription() {
        processor =
                processor(
                        SubscribedItems.of(subscribedItems),
                        ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        subscribedItems.add(Items.subscribedFrom("item3", new Object()));
        processor.process(record);
        // Verify that the update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);
    }

    @Test
    public void shouldNotAllowConcurrentProcessing() {
        processor =
                processor(
                        SubscribedItems.of(subscribedItems),
                        ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.allowConcurrentProcessing()).isTrue();
    }

    @Test
    public void shouldProcessADDCommand() {
        processor =
                processor(
                        SubscribedItems.of(subscribedItems),
                        ProcessUpdatesStrategy.autoCommandModeStrategy());

        // Subscribe to "item1" and process the record
        subscribedItems.add(Items.subscribedFrom("item1", new Object()));
        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isFalse();
        assertThat(smartConsumer.getLastUpdates()).containsExactly("command", "ADD");

        // Reset the counter
        smartConsumer.resetCounter();
        // Reset the snapshot flag
        smartConsumer.resetSnapshotEvent();

        // Add subscription "item2" and process the record
        subscribedItems.add(Items.subscribedFrom("item2", new Object()));
        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(smartConsumer.getCounter()).isEqualTo(2);
        assertThat(smartConsumer.getLastUpdates()).containsExactly("command", "ADD");
    }

    @Test
    public void shouldProcessDELETECommand() {
        processor =
                processor(
                        SubscribedItems.of(subscribedItems),
                        ProcessUpdatesStrategy.autoCommandModeStrategy());

        // Subscribe to "item1" and process the record
        subscribedItems.add(Items.subscribedFrom("item1", new Object()));
        processor.process(recordWithNullPayload);

        // Verify that the real-time update has been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isFalse();
        assertThat(smartConsumer.getLastUpdates()).containsExactly("command", "DELETE");
    }
}
