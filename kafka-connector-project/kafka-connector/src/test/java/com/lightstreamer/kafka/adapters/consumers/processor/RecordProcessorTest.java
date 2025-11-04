
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;

import static java.util.Collections.emptyMap;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.EventUpdater;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordRoutingStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
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

    private EventConsumer smartConsumer = new EventConsumer();
    private EventConsumer legacyConsumer = new EventConsumer();
    private MockItemEventListener listener;

    private SubscribedItems subscribedItems;
    private RecordProcessor<String, String> processor;

    @BeforeEach
    public void setUp() throws ExtractionException {
        // The mocked ItemEventListener instance, which updates the counter upon invocation
        this.listener = new MockItemEventListener(smartConsumer, legacyConsumer);
    }

    private RecordMapper<String, String> mapperForAutoCommandMode() {
        try {
            return builder()
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item1", emptyMap(), false, false))
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item2", emptyMap(), false, false))
                    .withFieldExtractor(
                            extractor(
                                    String(),
                                    "fields",
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
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item1", emptyMap(), false, false))
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item2", emptyMap(), false, false))
                    .withFieldExtractor(
                            extractor(
                                    String(),
                                    "fields",
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
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item1", emptyMap(), false, false))
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item2", emptyMap(), false, false))
                    .withFieldExtractor(
                            extractor(
                                    String(),
                                    "fields",
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
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item1", emptyMap(), false, false))
                    .withTemplateExtractor(
                            TEST_TOPIC, extractor(String(), "item2", emptyMap(), false, false))
                    .build();
        } catch (ExtractionException e) {
            throw new RuntimeException("Error building default mapper", e);
        }
    }

    RecordProcessor<String, String> processor(
            RecordMapper<String, String> mapper,
            SubscribedItems subscribedItems,
            ProcessUpdatesStrategy updatesStrategy) {
        return new DefaultRecordProcessor<>(
                mapper,
                EventUpdater.create(listener, !subscribedItems.acceptSubscriptions()),
                updatesStrategy,
                RecordRoutingStrategy.fromSubscribedItems(subscribedItems));
    }

    static Stream<Arguments> records() {
        return Stream.of(
                Arguments.of(
                        false,
                        defaultMapper(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-1"),
                        Map.of("aKey", "a", "aValue", "1a")),
                Arguments.of(
                        false,
                        defaultMapper(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-2"),
                        Map.of("aKey", "a", "aValue", "2a")),
                Arguments.of(
                        false,
                        mapperWithNoFieldsExtractor(),
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-2"),
                        Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource("records")
    public void shouldProcess(
            boolean allowImplicitItems,
            RecordMapper<String, String> mapper,
            KafkaRecord<String, String> record,
            Map<String, String> expectedFields) {
        subscribedItems = allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create();
        EventConsumer consumer = allowImplicitItems ? legacyConsumer : smartConsumer;
        processor = processor(mapper, subscribedItems, ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        // Subscribe to "item1" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item1", new Object()));
        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(consumer.getCounter()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(consumer.isSnapshotEvent()).isFalse();

        // Reset the counter
        consumer.resetCounter();
        // Reset the snapshot flag
        consumer.resetSnapshotEvent();

        // Add subscription "item2" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item2", new Object()));
        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(consumer.getCounter()).isEqualTo(2);
        assertThat(smartConsumer.getLastUpdates()).isEqualTo(expectedFields);
    }

    @Test
    public void shouldNotProcessUnexpectedSubscription() {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        defaultMapper(), subscribedItems, ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item3", new Object()));
        processor.process(Records.KafkaRecord(TEST_TOPIC, 0, "a-1"));
        // Verify that the update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);
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
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForAutoCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.autoCommandModeStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.AUTO_COMMAND_MODE);

        // Subscribe to "item1" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item1", new Object()));
        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(1);
        // Verify that the update has NOT been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isFalse();
        assertThat(smartConsumer.getLastUpdates()).isEqualTo(expectedFields);

        // Reset the counter
        smartConsumer.resetCounter();
        // Reset the snapshot flag
        smartConsumer.resetSnapshotEvent();

        // Add subscription "item2" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item2", new Object()));
        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(smartConsumer.getCounter()).isEqualTo(2);
        assertThat(smartConsumer.getLastUpdates()).isEqualTo(expectedFields);
    }

    static Stream<Arguments> commands() {
        return Stream.of(
                Arguments.of("ADD", false, 1),
                Arguments.of("UPDATE", false, 1),
                Arguments.of("DELETE", false, 1),
                Arguments.of("ADD", true, 2));
        // Arguments.of("UPDATE", true),
        // Arguments.of("DELETE", true));
    }

    @ParameterizedTest
    @MethodSource("commands")
    public void shouldProcessRecordWithAdmittedCommands(
            String command, boolean allowImplicitItems, int expectedUpdates) {
        subscribedItems = allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create();
        EventConsumer consumer = allowImplicitItems ? legacyConsumer : smartConsumer;
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.COMMAND);

        if (!allowImplicitItems) {
            // Subscribe to "item1" and process the record
            subscribedItems.addItem(Items.subscribedFrom("item1", new Object()));
        }

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that the nor clear snapshot neither end of snapshot were called
        if (allowImplicitItems) {
            assertThat(listener.legacyClearSnapshotCalled()).isFalse();
            assertThat(listener.legacyEndOfSnapshotCalled()).isFalse();

        } else {
            assertThat(listener.smartClearSnapshotCalled()).isFalse();
            assertThat(listener.smartEndOfSnapshotCalled()).isFalse();
        }

        // Verify that the real-time update has been routed
        assertThat(consumer.getCounter()).isEqualTo(expectedUpdates);
        assertThat(consumer.getLastUpdates()).containsExactly("key", "aKey", "command", command);

        // Verify that the update has been routed as a snapshot
        assertThat(consumer.isSnapshotEvent()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"CS", "EOS"})
    public void shouldNotProcessRecordWithNotAdmittedCommand(String command) {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        subscribedItems.addItem(Items.subscribedFrom("item1", new Object()));
        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that the real-time update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);

        // Double check that the update has NOT been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isFalse();

        // Verify that the nor clearSnapshot neither endOfSnapshot were called
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();
    }

    @Test
    public void shouldProcessRecordWithClearSnapshotCommand() {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(record);

        // Verify that only clearSnapshot was called
        assertThat(listener.smartClearSnapshotCalled()).isTrue();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();

        // Verify that the real-time update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);

        // Verify that the item keeps being a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ADD", "UPDATE", "DELETE"})
    public void shouldNotProcessRecordWithWrongCommandForSnapshot(String wrongCommand) {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
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
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();

        // Verify that the real-time update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @Test
    public void shouldProcessRecordWithEndOfSnapshotCommand() {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(record);

        // Verify only end of snapshot was called
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isTrue();

        // Verify that the real-time update has NOT been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(0);

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();
    }

    @Test
    public void shouldNotTriggerSnapshotEventAfterEOS() {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.addItem(item);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);

        // Verify that the real-time update has been routed
        assertThat(smartConsumer.getCounter()).isEqualTo(1);

        // Verify that the update has been routed as a snapshot
        assertThat(smartConsumer.isSnapshotEvent()).isTrue();

        // Then process a record containing an end of snapshot command
        var eosRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(eosRecord);

        // Verify that the end of snapshot was called
        assertThat(listener.smartEndOfSnapshotCalled()).isTrue();

        // Finally, process a records containing regulars commands, which should not trigger
        // snapshot events
        int currentCounter = smartConsumer.getCounter();
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            currentCounter++;
            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);
            // Verify that the real-time update has been routed
            assertThat(smartConsumer.getCounter()).isEqualTo(currentCounter);
            // Verify that the update has NOT been routed as a snapshot
            assertThat(smartConsumer.isSnapshotEvent()).isFalse();

            // Verify that the item is no longer a snapshot
            assertThat(item.isSnapshot()).isFalse();
        }
    }

    @Test
    public void shouldKeepSendingSnapshotAfterCS() {
        subscribedItems = SubscribedItems.create();
        processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.addItem(item);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);

        assertThat(smartConsumer.getCounter()).isEqualTo(1);
        assertThat(smartConsumer.isSnapshotEvent()).isTrue();

        // Then process a record containing a clear snapshot command
        var clsRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(clsRecord);
        assertThat(listener.smartClearSnapshotCalled()).isTrue();

        // Finally, process a records containing regulars commands, which should still trigger
        // snapshot events
        int currentCounter = smartConsumer.getCounter();
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            currentCounter++;
            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);
            // Verify that the real-time update has been routed
            assertThat(smartConsumer.getCounter()).isEqualTo(currentCounter);

            // Verify that the update has been routed as a snapshot
            assertThat(smartConsumer.isSnapshotEvent()).isTrue();

            // Verify that the item is still a snapshot
            assertThat(item.isSnapshot()).isTrue();
        }
    }
}
