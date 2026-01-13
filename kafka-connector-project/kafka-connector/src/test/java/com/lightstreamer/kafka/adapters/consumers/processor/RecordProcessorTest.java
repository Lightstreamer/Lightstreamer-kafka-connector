
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.canonicalItemExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.namedFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordRoutingStrategy;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.UpdateCall;
import com.lightstreamer.kafka.test_utils.Records;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

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

    private MockItemEventListener eventListener;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.eventListener = new MockItemEventListener();
    }

    private RecordMapper<String, String> mapperForAutoCommandMode() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .withFieldExtractor(
                            namedFieldsExtractor(
                                    String(),
                                    Map.of(
                                            "key", // Auto Command Mode requires "key" field
                                            Wrapped("#{KEY}"),
                                            "valueField",
                                            Wrapped("#{VALUE}")),
                                    false,
                                    false))
                    .build();
        } catch (ExtractionException e) {
            throw new RuntimeException("Error building auto command mode mapper", e);
        }
    }

    private static RecordMapper<String, String> mapperForCommandMode() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .withFieldExtractor(
                            namedFieldsExtractor(
                                    String(),
                                    // Command Mode requires "key" and "command" fields
                                    Map.of(
                                            "key",
                                            Wrapped("#{KEY}"),
                                            "command",
                                            Wrapped("#{VALUE}")),
                                    false,
                                    false))
                    .build();
        } catch (ExtractionException e) {
            throw new RuntimeException("Error building command mode mapper", e);
        }
    }

    private static RecordMapper<String, String> defaultMapper() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .withFieldExtractor(
                            namedFieldsExtractor(
                                    String(),
                                    Map.of(
                                            "aKey",
                                            Wrapped("#{KEY}"),
                                            "aValue",
                                            Wrapped("#{VALUE}")),
                                    false,
                                    false))
                    .build();
        } catch (ExtractionException e) {
            throw new RuntimeException("Error building default mapper", e);
        }
    }

    private static RecordMapper<String, String> mapperWithNoFieldsExtractor() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .build();
        } catch (ExtractionException e) {
            throw new RuntimeException("Error building default mapper", e);
        }
    }

    RecordProcessor<String, String> processor(
            RecordMapper<String, String> mapper,
            EventListener listener,
            SubscribedItems subscribedItems,
            ProcessUpdatesStrategy updatesStrategy) {
        return new DefaultRecordProcessor<>(
                mapper,
                listener,
                updatesStrategy,
                RecordRoutingStrategy.fromSubscribedItems(subscribedItems));
    }

    static Stream<Arguments> records() {
        return Stream.of(
                Arguments.of(
                        defaultMapper(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-1"),
                        Map.of("aKey", "a", "aValue", "1a")),
                Arguments.of(
                        defaultMapper(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-2"),
                        Map.of("aKey", "a", "aValue", "2a")),
                Arguments.of(
                        mapperWithNoFieldsExtractor(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-2"),
                        Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource("records")
    public void shouldProcess(
            RecordMapper<String, String> mapper,
            KafkaRecord<String, String> record,
            Map<String, String> expectedFields) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapper,
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        SubscribedItem item1 = Items.subscribedFrom("item1", itemHandle1);
        subscribedItems.addItem(item1);
        item1.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(this.eventListener.getSmartRealtimeUpdateCount()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(this.eventListener.getSmartSnapshotUpdates()).isEmpty();

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        SubscribedItem item2 = Items.subscribedFrom("item2", itemHandle2);
        subscribedItems.addItem(item2);
        item2.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getSmartRealtimeUpdateCount()).isEqualTo(2);
        assertThat(this.eventListener.getSmartRealtimeUpdates())
                .containsExactly(
                        new UpdateCall(itemHandle1, expectedFields, false),
                        new UpdateCall(itemHandle2, expectedFields, false));
    }

    @Test
    public void shouldNotProcessUnexpectedSubscription() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        defaultMapper(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        SubscribedItem item = Items.subscribedFrom("item3", new Object());
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        processor.process(Records.KafkaRecord(TEST_TOPIC, 0, "a-1"));
        // Verify that the update has NOT been routed
        assertThat(eventListener.getAllUpdatesChronological()).isEmpty();
    }

    static Stream<Arguments> recordsForAutoCommandMode() {
        return Stream.of(
                Arguments.of(
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-1"),
                        Map.of("key", "a", "valueField", "1a", "command", "ADD")),
                Arguments.of(
                        Records.KafkaRecord(TEST_TOPIC, "a", null),
                        Map.of("key", "a", "command", "DELETE")));
    }

    @ParameterizedTest
    @MethodSource("recordsForAutoCommandMode")
    public void shouldProcessRecordWithAutoCommandMode(
            KafkaRecord<String, String> record, Map<String, String> expectedFields) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForAutoCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.autoCommandModeStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.AUTO_COMMAND_MODE);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        SubscribedItem item1 = Items.subscribedFrom("item1", itemHandle1);
        subscribedItems.addItem(item1);
        item1.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(eventListener.getSmartRealtimeUpdateCount()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(eventListener.getSmartSnapshotUpdates()).isEmpty();
        assertThat(eventListener.getSmartRealtimeUpdates())
                .containsExactly(new UpdateCall(itemHandle1, expectedFields, false));

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        SubscribedItem item2 = Items.subscribedFrom("item2", itemHandle2);
        subscribedItems.addItem(item2);
        item2.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getSmartRealtimeUpdateCount()).isEqualTo(2);
        assertThat(this.eventListener.getSmartRealtimeUpdates())
                .containsExactly(
                        new UpdateCall(itemHandle1, expectedFields, false),
                        new UpdateCall(itemHandle2, expectedFields, false));
    }

    static Stream<Arguments> commands() {
        return Stream.of(
                Arguments.of("ADD", 1), Arguments.of("UPDATE", 1), Arguments.of("DELETE", 1));
    }

    @ParameterizedTest
    @MethodSource("commands")
    public void shouldProcessRecordWithAdmittedCommands(String command, int expectedUpdates) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.COMMAND);

        // Subscribe to "item1" and process the record
        Object itemHandle = new Object();
        SubscribedItem item = Items.subscribedFrom("item1", itemHandle);
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that the nor clearSnapshot neither endOfSnapshot were called
        assertThat(this.eventListener.getSmartClearSnapshotCalls()).isEmpty();
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls()).isEmpty();

        // Verify that the real-time update has been routed
        assertThat(this.eventListener.getAllUpdatesChronological()).hasSize(expectedUpdates);
        assertThat(this.eventListener.getSmartSnapshotUpdates())
                .containsExactly(
                        new UpdateCall(
                                itemHandle, Map.of("command", command, "key", "aKey"), true));
        // Verify that the update has been routed as a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"CS", "EOS"})
    public void shouldNotProcessRecordWithNotAdmittedCommand(String command) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener); // Mark the subscription as snapshot processed

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that no events have been routed
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();

        // Verify that the nor clearSnapshot neither endOfSnapshot were called
        assertThat(this.eventListener.getSmartClearSnapshotCalls()).isEmpty();
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls()).isEmpty();
    }

    @Test
    public void shouldProcessRecordWithClearSnapshotCommand() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(record);

        // Verify that only clearSnapshot was called
        assertThat(this.eventListener.getSmartClearSnapshotCalls()).hasSize(1);
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls()).isEmpty();

        // Verify that no events have been routed
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();

        // Verify that the item keeps being a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ADD", "UPDATE", "DELETE"})
    public void shouldNotProcessRecordWithWrongCommandForSnapshot(String wrongCommand) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        // Consume a record with a "snapshot" key and a wrong command
        KafkaRecord<String, String> record =
                Records.KafkaRecord(TEST_TOPIC, "snapshot", wrongCommand);
        processor.process(record);

        // Verify that the nor clear snapshot neither end of snapshot were called
        assertThat(this.eventListener.getSmartClearSnapshotCalls()).isEmpty();
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls()).isEmpty();

        // Verify that no events have been routed
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @Test
    public void shouldProcessRecordWithEndOfSnapshotCommand() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(record);

        // Verify that only endOfSnapshot was called
        assertThat(this.eventListener.getSmartClearSnapshotCalls()).isEmpty();
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls()).hasSize(1);

        // Verify that no events have been routed
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();
    }

    @Test
    public void shouldNotTriggerSnapshotEventAfterEOS() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the real-time update has NOT been routed
        assertThat(this.eventListener.getSmartRealtimeUpdateCount()).isEqualTo(0);

        // Verify that the update has been routed as a snapshot
        assertThat(this.eventListener.getSmartSnapshotUpdates()).hasSize(1);

        // Reset the event listener
        this.eventListener.reset();

        // Then process a record containing an endOfSnapshot command
        var eosRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(eosRecord);

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();

        // Verify that the endOfSnapshot was called
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();
        assertThat(this.eventListener.getSmartEndOfSnapshotCalls())
                .containsExactly(item.itemHandle());

        // Finally, process a records containing regulars commands, which should NOT trigger
        // snapshot events
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            // Reset the event listener
            this.eventListener.reset();
            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);

            // Verify that the update as has been routed as real-time update
            assertThat(this.eventListener.getAllUpdatesChronological()).hasSize(1);
            assertThat(this.eventListener.getSmartRealtimeUpdates())
                    .containsExactly(
                            new UpdateCall(
                                    item.itemHandle(),
                                    Map.of("command", command, "key", "aKey"),
                                    false));

            // Verify that the update has NOT been routed as a snapshot
            assertThat(this.eventListener.getSmartSnapshotUpdates()).isEmpty();

            // Double check that the item isn't still a snapshot
            assertThat(item.isSnapshot()).isFalse();
        }
    }

    @Test
    public void shouldKeepSendingSnapshotAfterCS() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        EventListener listener = EventListener.smartEventListener(this.eventListener);
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        listener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.addItem(item);
        item.enableRealtimeEvents(listener);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the real-time update has NOT been routed
        assertThat(this.eventListener.getSmartRealtimeUpdateCount()).isEqualTo(0);

        // Verify that the update has been routed as a snapshot
        assertThat(this.eventListener.getSmartSnapshotUpdates()).hasSize(1);

        // Reset the event listener
        this.eventListener.reset();

        // Then process a record containing a clearSnapshot command
        var clsRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(clsRecord);

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the clearSnapshot was called
        assertThat(this.eventListener.getAllUpdatesChronological()).isEmpty();
        assertThat(this.eventListener.getSmartClearSnapshotCalls())
                .containsExactly(item.itemHandle());

        // Finally, process a records containing regulars commands, which should still trigger
        // snapshot events
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            // Reset the event listener
            this.eventListener.reset();

            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);

            // Verify that the update has been routed as a snapshot
            assertThat(this.eventListener.getAllUpdatesChronological()).hasSize(1);
            assertThat(this.eventListener.getSmartSnapshotUpdates()).hasSize(1);
            assertThat(this.eventListener.getSmartSnapshotUpdates())
                    .containsExactly(
                            new UpdateCall(
                                    item.itemHandle(),
                                    Map.of("command", command, "key", "aKey"),
                                    true));

            // Double check that the item is still a snapshot
            assertThat(item.isSnapshot()).isTrue();
        }
    }
}
