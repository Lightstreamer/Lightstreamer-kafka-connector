
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
import static com.lightstreamer.kafka.test_utils.Mocks.EventCall.EventType.UPDATE;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordProcessorImpl;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ForceableSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks.EventCall;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RecordProcessorTest {

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
            ItemEventListener listener,
            SubscribedItems subscribedItems,
            ProcessUpdatesStrategy updatesStrategy) {
        return new RecordProcessorImpl<>(mapper, subscribedItems, listener, updatesStrategy);
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
                        Map.of()));
    }

    @ParameterizedTest
    @MethodSource("records")
    public void shouldProcess(
            RecordMapper<String, String> mapper,
            KafkaRecord<String, String> record,
            Map<String, String> expectedFields) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapper,
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        OnDemandSubscribedItem item1 = Items.onDemandSubscribedItem("item1", itemHandle1);
        subscribedItems.addItem(item1);

        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(this.eventListener.getEvents())
                .containsExactly(new EventCall(UPDATE, itemHandle1, expectedFields, false));
        // Verify that the update has NOT been routed as a snapshot
        assertThat(this.eventListener.getSmartSnapshotUpdates()).isEmpty();

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        OnDemandSubscribedItem item2 = Items.onDemandSubscribedItem("item2", itemHandle2);
        subscribedItems.addItem(item2);

        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(UPDATE, itemHandle1, expectedFields, false),
                        new EventCall(UPDATE, itemHandle2, expectedFields, false));
    }

    @ParameterizedTest
    @MethodSource("records")
    public void shouldProcessForcedSubscriptions(
            RecordMapper<String, String> mapper,
            KafkaRecord<String, String> record,
            Map<String, String> expectedFields) {
        ForceableSubscribedItems subscribedItems = SubscribedItems.forceable(eventListener, null);
        RecordProcessor<String, String> processor =
                processor(
                        mapper,
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        processor.process(record);

        // Simulate the forced subscription to "item1" triggered by the record processing
        Object itemHandle1 = new Object();
        subscribedItems.activateOrInstall("item1", itemHandle1);

        // Simulate the forced subscription to "item2" triggered by the record processing
        Object itemHandle2 = new Object();
        subscribedItems.activateOrInstall("item2", itemHandle2);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(UPDATE, itemHandle1, expectedFields, false),
                        new EventCall(UPDATE, itemHandle2, expectedFields, false));
    }

    @Test
    public void shouldNotProcessUnexpectedSubscription() {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        defaultMapper(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item3", new Object());
        subscribedItems.addItem(item);

        processor.process(Records.KafkaRecord(TEST_TOPIC, 0, "a-1"));

        // Verify that no events have been routed, since the record doesn't match any of the
        // subscribed items
        assertThat(eventListener.getEvents()).isEmpty();
    }

    @Test
    public void shouldNotProcessUnexpectedSubscriptionWithForcedSubscription() {
        ForceableSubscribedItems subscribedItems = SubscribedItems.forceable(eventListener, null);
        RecordProcessor<String, String> processor =
                processor(
                        defaultMapper(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item3", new Object());
        subscribedItems.activateOrInstall("item3", item);

        processor.process(Records.KafkaRecord(TEST_TOPIC, 0, "a-1"));

        // Simulate the forced subscription to "item1" triggered by the record processing
        Object itemHandle1 = new Object();
        subscribedItems.activateOrInstall("item1", itemHandle1);

        // Simulate the forced subscription to "item2" triggered by the record processing
        Object itemHandle2 = new Object();
        subscribedItems.activateOrInstall("item2", itemHandle2);

        // Verify that the update has been routed only for the forced subscriptions "item1" and
        // "item2", but not for the unexpected "item3"
        assertThat(eventListener.getEvents())
                .containsExactly(
                        new EventCall(
                                UPDATE, itemHandle1, Map.of("aKey", "a", "aValue", "1a"), false),
                        new EventCall(
                                UPDATE, itemHandle2, Map.of("aKey", "a", "aValue", "1a"), false));
    }

    static Stream<Arguments> recordsForAutoCommandMode() {
        return Stream.of(
                Arguments.of(
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-1"),
                        Map.of("key", "a", "valueField", "1a", "command", "ADD")),
                Arguments.of(
                        Records.StringKafkaRecord(TEST_TOPIC, "a", null),
                        Map.of("key", "a", "command", "DELETE")));
    }

    @ParameterizedTest
    @MethodSource("recordsForAutoCommandMode")
    public void shouldProcessRecordWithAutoCommandMode(
            KafkaRecord<String, String> record, Map<String, String> expectedFields) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForAutoCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.autoCommandModeStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.AUTO_COMMAND_MODE);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        OnDemandSubscribedItem item1 = Items.onDemandSubscribedItem("item1", itemHandle1);
        subscribedItems.addItem(item1);

        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(eventListener.getEvents())
                .containsExactly(new EventCall(UPDATE, itemHandle1, expectedFields, false));

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        OnDemandSubscribedItem item2 = Items.onDemandSubscribedItem("item2", itemHandle2);
        subscribedItems.addItem(item2);

        processor.process(record);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(UPDATE, itemHandle1, expectedFields, false),
                        new EventCall(UPDATE, itemHandle2, expectedFields, false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ADD", "UPDATE", "DELETE"})
    public void shouldProcessRecordWithAdmittedCommands(String command) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.COMMAND);

        // Subscribe to "item1" and process the record
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that the real-time update has been routed
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(
                                UPDATE,
                                itemHandle,
                                Map.of("command", command, "key", "aKey"),
                                true));

        assertThat(item.isSnapshot()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"CS", "EOS"})
    public void shouldNotProcessRecordWithNotAdmittedCommand(String command) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", new Object());
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that no events have been routed
        assertThat(this.eventListener.getEvents()).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void shouldNotProcessRecordWithBlankOrNullKey(String key) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", new Object());
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, key, "ADD");
        processor.process(record);

        // Verify that no events have been routed
        assertThat(this.eventListener.getEvents()).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void shouldNotProcessRecordWithBlankOrNullCommand(String command) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", new Object());
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);

        // Verify that no events have been routed
        assertThat(this.eventListener.getEvents()).isEmpty();
    }

    @Test
    public void shouldProcessRecordWithClearSnapshotCommand() {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(record);

        // Verify that only clearSnapshot was called
        assertThat(this.eventListener.getEvents()).containsExactly(EventCall.CS(itemHandle));

        // Verify that the item keeps being a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @Test
    public void shouldProcessRecordWithEndOfSnapshotCommand() {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        KafkaRecord<String, String> record = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(record);

        // Verify that only endOfSnapshot was called
        assertThat(this.eventListener.getEvents()).containsExactly(EventCall.EOS(itemHandle));

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ADD", "UPDATE", "DELETE", "UNKNOWN"})
    public void shouldNotProcessRecordWithWrongCommandForSnapshot(String wrongCommand) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1" and process the record
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.addItem(item);

        // Consume a record with a "snapshot" key and a wrong command
        KafkaRecord<String, String> record =
                Records.KafkaRecord(TEST_TOPIC, "snapshot", wrongCommand);
        processor.process(record);

        // Verify that neither clearSnapshot nor endOfSnapshot were called
        assertThat(this.eventListener.getEvents()).isEmpty();

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @Test
    public void shouldSnapshotFollowCS() {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        subscribedItems.addItem(item);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the update has been routed as a snapshot
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(
                                UPDATE, itemHandle, Map.of("command", "ADD", "key", "aKey"), true));

        // Reset the event listener
        this.eventListener.reset();

        // Then process a record containing a clearSnapshot command
        var clsRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(clsRecord);

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the clearSnapshot was called
        assertThat(this.eventListener.getEvents())
                .containsExactly(new EventCall(EventCall.EventType.CS, itemHandle, null, false));

        // Finally, process a records containing regulars commands, which should still trigger
        // snapshot events
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            // Reset the event listener
            this.eventListener.reset();

            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);

            // Verify that the update has been routed as a snapshot
            assertThat(this.eventListener.getEvents())
                    .containsExactly(
                            new EventCall(
                                    UPDATE,
                                    itemHandle,
                                    Map.of("command", command, "key", "aKey"),
                                    true));

            // Double check that the item is still a snapshot
            assertThat(item.isSnapshot()).isTrue();
        }
    }

    @Test
    public void shouldSnapshotFollowEOS() {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        this.eventListener,
                        subscribedItems,
                        ProcessUpdatesStrategy.commandStrategy());

        // Subscribe to "item1"
        Object itemHandle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem("item1", itemHandle);
        subscribedItems.addItem(item);

        // Process a record containing a regular command
        var addRecord = Records.KafkaRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);
        assertThat(item.isSnapshot()).isTrue();

        // Verify that the update has been routed as a snapshot
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(
                                UPDATE, itemHandle, Map.of("command", "ADD", "key", "aKey"), true));

        // Reset the event listener
        this.eventListener.reset();

        // Then process a record containing an endOfSnapshot command
        var eosRecord = Records.KafkaRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(eosRecord);

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();

        // Verify that the endOfSnapshot was called
        assertThat(this.eventListener.getEvents()).containsExactly(EventCall.EOS(itemHandle));

        // Finally, process a records containing regulars commands, which should NOT trigger
        // snapshot events
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            // Reset the event listener
            this.eventListener.reset();
            var record = Records.KafkaRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);

            // Verify that the update as has been routed as real-time update
            assertThat(this.eventListener.getEvents())
                    .containsExactly(
                            new EventCall(
                                    UPDATE,
                                    itemHandle,
                                    Map.of("command", command, "key", "aKey"),
                                    false));

            // Double check that the item isn't a snapshot anymore
            assertThat(item.isSnapshot()).isFalse();
        }
    }
}
