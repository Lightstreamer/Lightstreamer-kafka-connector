
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
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordProcessorTest {

    private static final String TEST_TOPIC = "topic";

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    private RecordMapper<String, String> mapper;
    private AtomicInteger counter;
    private MockItemEventListener listener;
    private ConsumerRecord<String, String> record;
    private Set<SubscribedItem> subscribedItems;
    private RecordProcessor<String, String> processor;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                extractor(String(), "item1", Collections.emptyMap(), false))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                extractor(String(), "item2", Collections.emptyMap(), false))
                        .build();
        // Counts the listener invocations to deliver the real-time updates
        this.counter = new AtomicInteger();

        // The mocked ItemEventListener instance, wich updates the counter upon invocation.
        this.listener = new MockItemEventListener(update -> counter.incrementAndGet());

        // A record routable to "item1" and "item2"
        this.record = Records.Record(TEST_TOPIC, 0, "a-1");

        // The collection of subscribable items
        this.subscribedItems = new HashSet<>();

        // The RecordProcessor instance
        this.processor =
                new RecordConsumerSupport.DefaultRecordProcessor<>(
                        mapper, subscribedItems, listener);
    }

    @Test
    public void shouldProcess() {
        // Subscribe to "item1" and process the record
        subscribedItems.add(Items.subscribedFrom("item1", new Object()));
        processor.process(record);
        // Verify that the real-time update has been routed
        assertThat(counter.get()).isEqualTo(1);
        // Reset the counter
        counter.set(0);

        // Add subscription "item2"
        subscribedItems.add(Items.subscribedFrom("item2", new Object()));
        processor.process(record);
        // Verify that the update has been routed two times, one for "item1" and one for "item2"
        assertThat(counter.get()).isEqualTo(2);
    }

    @Test
    public void shouldNotProcess() {
        // Subscribe to the unexpected "item3" and process the record
        subscribedItems.add(Items.subscribedFrom("item3", new Object()));
        processor.process(record);
        // Verify that the update has NOT been routed
        assertThat(counter.get()).isEqualTo(0);
    }
}
