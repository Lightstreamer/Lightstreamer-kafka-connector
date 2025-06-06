
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
import static com.lightstreamer.kafka.common.expressions.Expressions.Wrapped;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandRecordProcessor;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;
import com.lightstreamer.kafka.test_utils.Records;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CommandRecordProcessorTest {

    private static final String TEST_TOPIC = "topic";

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    private RecordMapper<String, String> mapper;

    private AtomicInteger counter;
    private AtomicBoolean snapshotEvent;
    private MockItemEventListener listener;

    private Set<SubscribedItem> subscribedItems;
    private CommandRecordProcessor<String, String> processor;
    private SubscribedItems sub;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                extractor(String(), "item1", Collections.emptyMap(), false, false))
                        .withFieldExtractor(
                                DataExtractor.extractor(
                                        String(),
                                        "fields",
                                        Map.of(
                                                "key",
                                                Wrapped("#{KEY}"),
                                                "command",
                                                Wrapped("#{VALUE}")),
                                        false,
                                        false))
                        .build();
        // Counts the listener invocations to deliver the real-time updates
        this.counter = new AtomicInteger();

        // Indicates whether the delivered event is a snapshot
        this.snapshotEvent = new AtomicBoolean();

        // The mocked ItemEventListener instance, which updates the counter upon invocation
        this.listener =
                new MockItemEventListener(
                        (update, isSnapshot) -> {
                            counter.incrementAndGet();
                            snapshotEvent.set(isSnapshot);
                        });

        // The collection of subscribable items
        this.subscribedItems = new HashSet<>();
        this.sub = () -> subscribedItems.iterator();

        // The RecordProcessor instance
        this.processor = commandRecordProcessor();
    }

    private CommandRecordProcessor<String, String> commandRecordProcessor() {
        return new RecordConsumerSupport.CommandRecordProcessor<>(mapper, sub, listener);
    }

    @Test
    public void shouldBeCommandEnforceEnabled() {
        assertThat(processor.isCommandEnforceEnabled()).isTrue();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    KEY        | COMMAND     | EXPECTED
                    aKey       | ADD         | true
                    aKey       | UPDATE      | true
                    aKey       | DELETE      | true
                    aKey       | UNKNOWN     | false
                    aKey       | <NOCOMMAND> | false
                    aKey       | <EMPTY>     | false
                    aKey       | CS          | false
                    aKey       | EOS         | false
                    aKey       | <NULL>      | false
                    <NOKEY>    | ADD         | false
                    <NULL>     | ADD         | false
                    <NULL>     | UPDATE      | false
                    <NULL>     | DELETE      | false
                    <EMPTY>    | DELETE      | false
                    snapshot   | CS          | true
                    snapshot   | EOS         | true
                    snapshot   | ADD         | false
                    snapshot   | DELETE      | false
                    snapshot   | UPDATE      | false
                    snapshot   | UNKNOWN     | false
                    """)
    void shouldCheckInvalidInput(String key, String command, boolean expected) {
        Map<String, String> input = new HashMap<>();
        switch (key) {
            case "<NOKEY>" -> {}
            case "<EMPTY>" -> input.put("key", "");
            case "<NULL>" -> input.put("key", null);
            default -> input.put("key", key);
        }

        switch (command) {
            case "<NOCOMMAND>" -> {}
            case "<EMPTY>" -> input.put("command", "");
            case "<NULL>" -> input.put("command", null);
            default -> input.put("command", command);
        }

        assertThat(processor.checkInput(input)).isEqualTo(expected);
    }

    @Test
    public void shouldDeliverCommand() {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.add(item);

        int c = 0;
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            c++;
            var record = Records.ConsumerRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);
            // Verify that nor clear snapshot neither end of snapshot were called
            assertThat(listener.smartClearSnapshotCalled()).isFalse();
            assertThat(listener.smartEndOfSnapshotCalled()).isFalse();

            // Verify that the real-time update has been routed
            assertThat(counter.get()).isEqualTo(c);

            // Verify that the update has been routed as a snapshot
            assertThat(snapshotEvent.get()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"CS", "EOS"})
    public void shouldNotNotDeliverNotAdmittedCommand(String command) {
        subscribedItems.add(Items.subscribedFrom("item1", new Object()));
        var record = Records.ConsumerRecord(TEST_TOPIC, "aKey", command);
        processor.process(record);
        assertThat(counter.get()).isEqualTo(0);
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();
    }

    @Test
    public void shouldHandleClearSnapshotCommand() {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.add(item);
        var record = Records.ConsumerRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(record);

        // Verify that the only clear snapshot was called
        assertThat(listener.smartClearSnapshotCalled()).isTrue();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();

        // Verify that the real-time update has NOT been routed
        assertThat(counter.get()).isEqualTo(0);

        // Verify that the item keeps being a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ADD", "UPDATE", "DELETE"})
    public void shouldNotHandleSnapshotKeyWithWrongCommand(String wrongCommand) {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.add(item);

        // Consume a record with a "snapshot" key and a wrong command
        var record = Records.ConsumerRecord(TEST_TOPIC, "snapshot", wrongCommand);
        processor.process(record);

        // Verify that the nor clear snapshot neither end of snapshot were called
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isFalse();

        // Verify that the real-time update has NOT been routed
        assertThat(counter.get()).isEqualTo(0);

        // Verify that the item is still a snapshot
        assertThat(item.isSnapshot()).isTrue();
    }

    @Test
    public void shouldHandleEndOfSnapshotCommand() {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        assertThat(item.isSnapshot()).isTrue();
        subscribedItems.add(item);
        var record = Records.ConsumerRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(record);

        // Verify that the only end of snapshot was called
        assertThat(listener.smartClearSnapshotCalled()).isFalse();
        assertThat(listener.smartEndOfSnapshotCalled()).isTrue();

        // Verify that the real-time update has NOT been routed
        assertThat(counter.get()).isEqualTo(0);

        // Verify that the item is no longer a snapshot
        assertThat(item.isSnapshot()).isFalse();
    }

    @Test
    public void shouldNotTriggerSnapshotEventAfterEOS() {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.add(item);

        // Process a record containing a regular command
        var addRecord = Records.ConsumerRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);

        // Verify that the real-time update has been routed
        assertThat(counter.get()).isEqualTo(1);

        // Verify that the update has been routed as a snapshot
        assertThat(snapshotEvent.get()).isTrue();

        // Then process a record containing an end of snapshot command
        var eosRecord = Records.ConsumerRecord(TEST_TOPIC, "snapshot", "EOS");
        processor.process(eosRecord);

        // Verify that the end of snapshot was called
        assertThat(listener.smartEndOfSnapshotCalled()).isTrue();

        // Finally, process a records containing regulars commands, which should not trigger
        // snapshot events
        int currentCounter = counter.get();
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            currentCounter++;
            var record = Records.ConsumerRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);
            // Verify that the real-time update has been routed
            assertThat(counter.get()).isEqualTo(currentCounter);
            // Verify that the update has NOT been routed as a snapshot
            assertThat(snapshotEvent.get()).isFalse();

            // Verify that the item is no longer a snapshot
            assertThat(item.isSnapshot()).isFalse();
        }
    }

    @Test
    public void shouldKeepSendingSnapshotAfterCS() {
        SubscribedItem item = Items.subscribedFrom("item1", new Object());
        subscribedItems.add(item);

        // Process a record containing a regular command
        var addRecord = Records.ConsumerRecord(TEST_TOPIC, "aKey", "ADD");
        processor.process(addRecord);
        assertThat(counter.get()).isEqualTo(1);
        assertThat(snapshotEvent.get()).isTrue();

        // Then process a record containing a clear snapshot command
        var clsRecord = Records.ConsumerRecord(TEST_TOPIC, "snapshot", "CS");
        processor.process(clsRecord);
        assertThat(listener.smartClearSnapshotCalled()).isTrue();

        // Finally, process a records containing regulars commands, which should still trigger
        // snapshot events
        int currentCounter = counter.get();
        for (String command : List.of("ADD", "UPDATE", "DELETE")) {
            currentCounter++;
            var record = Records.ConsumerRecord(TEST_TOPIC, "aKey", command);
            processor.process(record);
            // Verify that the real-time update has been routed
            assertThat(counter.get()).isEqualTo(currentCounter);

            // Verify that the update has been routed as a snapshot
            assertThat(snapshotEvent.get()).isTrue();

            // Verify that the item is still a snapshot
            assertThat(item.isSnapshot()).isTrue();
        }
    }
}
