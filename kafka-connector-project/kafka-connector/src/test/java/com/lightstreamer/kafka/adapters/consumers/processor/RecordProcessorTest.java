
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.test_utils.Mocks.EventCall.EventType.UPDATE;

import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RealtimeDeliveryStrategy;
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
import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class RecordProcessorTest {

    private static final String TEST_TOPIC = "topic";
    private static final Logger logger = LogFactory.getLogger("TestConnection");

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    private MockItemEventListener eventListener;
    private RealtimeDeliveryStrategy deliveryStrategy;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.eventListener = new MockItemEventListener();
        this.deliveryStrategy = new RealtimeDeliveryStrategy(eventListener);
    }

    private RecordMapper<String, String> mapperForCommandMode() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .fieldExtractor(
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

    private static RecordMapper<String, String> defaultMapper() {
        try {
            return builder()
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item1")))
                    .addCanonicalItemExtractor(
                            TEST_TOPIC,
                            canonicalItemExtractor(String(), Expressions.EmptyTemplate("item2")))
                    .fieldExtractor(
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
            SubscribedItems subscribedItems,
            ProcessUpdatesStrategy updatesStrategy) {
        return new RecordProcessorImpl<>(mapper, subscribedItems, updatesStrategy);
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
                processor(mapper, subscribedItems, ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        OnDemandSubscribedItem item1 =
                Items.onDemandSubscribedFrom(Subscription("item1"), itemHandle1);
        subscribedItems.addItem(item1);

        processor.process(record, deliveryStrategy);

        // Verify that the real-time update has been routed
        assertThat(this.eventListener.getEvents())
                .containsExactly(new EventCall(UPDATE, itemHandle1, expectedFields, false));
        // Verify that the update has NOT been routed as a snapshot
        assertThat(this.eventListener.getSmartSnapshotUpdates()).isEmpty();

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        OnDemandSubscribedItem item2 =
                Items.onDemandSubscribedFrom(Subscription("item2"), itemHandle2);
        subscribedItems.addItem(item2);

        processor.process(record, deliveryStrategy);

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
        ForceableSubscribedItems subscribedItems = SubscribedItems.forceable(eventListener, logger);
        RecordProcessor<String, String> processor =
                processor(mapper, subscribedItems, ProcessUpdatesStrategy.defaultStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.DEFAULT);

        processor.process(record, deliveryStrategy);

        // Simulate the forced subscription to "item1" triggered by the record processing
        Object itemHandle1 = new Object();
        subscribedItems.activateOrInstall(Subscription("item1"), itemHandle1);

        // Simulate the forced subscription to "item2" triggered by the record processing
        Object itemHandle2 = new Object();
        subscribedItems.activateOrInstall(Subscription("item2"), itemHandle2);

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
                        defaultMapper(), subscribedItems, ProcessUpdatesStrategy.defaultStrategy());

        // Subscribe to the unexpected "item3" and process the record
        OnDemandSubscribedItem item =
                Items.onDemandSubscribedFrom(Subscription("item3"), new Object());
        subscribedItems.addItem(item);

        processor.process(Records.KafkaRecord(TEST_TOPIC, 0, "a-1"), deliveryStrategy);

        // Verify that no events have been routed, since the record doesn't match any of the
        // subscribed items
        assertThat(eventListener.getEvents()).isEmpty();
    }

    static Stream<Arguments> recordsForCommandMode() {
        return Stream.of(
                Arguments.of(
                        Records.KafkaRecord(TEST_TOPIC, 0, "a-1"),
                        Map.of("key", "a", "valueField", "1a", "command", "ADD")),
                Arguments.of(
                        Records.StringKafkaRecord(TEST_TOPIC, "a", null),
                        new LinkedHashMap<>() {
                            {
                                put("key", "a");
                                put("command", "DELETE");
                            }
                        }),
                Arguments.of(
                        Records.StringKafkaRecord(TEST_TOPIC, null, null),
                        // Actually, this update is invalid as the Lightstreamer kernel would reject
                        // an event with a null key in COMMAND mode, but we want to test that the
                        // processor can
                        // handle it gracefully
                        new LinkedHashMap<>() {
                            {
                                put("key", null);
                                put("command", "DELETE");
                            }
                        }));
    }

    @ParameterizedTest
    @MethodSource("recordsForCommandMode")
    public void shouldProcessRecordWithCommandMode(
            KafkaRecord<String, String> record, Map<String, String> expectedFields) {
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        RecordProcessor<String, String> processor =
                processor(
                        mapperForCommandMode(),
                        subscribedItems,
                        ProcessUpdatesStrategy.commandModeStrategy());
        assertThat(processor.processUpdatesType()).isEqualTo(ProcessUpdatesType.COMMAND_MODE);

        // Subscribe to "item1" and process the record
        Object itemHandle1 = new Object();
        OnDemandSubscribedItem item1 =
                Items.onDemandSubscribedFrom(Subscription("item1"), itemHandle1);
        subscribedItems.addItem(item1);

        processor.process(record, deliveryStrategy);

        // Verify that the real-time update has been routed
        assertThat(eventListener.getEvents())
                .containsExactly(new EventCall(UPDATE, itemHandle1, expectedFields, false));

        // Reset the counter
        this.eventListener.reset();

        // Add subscription "item2" and process the record
        Object itemHandle2 = new Object();
        OnDemandSubscribedItem item2 =
                Items.onDemandSubscribedFrom(Subscription("item2"), itemHandle2);
        subscribedItems.addItem(item2);

        processor.process(record, deliveryStrategy);

        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(this.eventListener.getEvents())
                .containsExactly(
                        new EventCall(UPDATE, itemHandle1, expectedFields, false),
                        new EventCall(UPDATE, itemHandle2, expectedFields, false));
    }
}
