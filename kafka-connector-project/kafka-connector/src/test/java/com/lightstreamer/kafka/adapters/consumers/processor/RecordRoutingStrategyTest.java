
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
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.canonicalItemExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.EmptyTemplate;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecord;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordRoutingStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class RecordRoutingStrategyTest {

    private static final String TEST_TOPIC_1 = "topic";

    private RecordMapper<String, String> mapper;

    @BeforeEach
    public void setUp() throws ExtractionException {
        this.mapper =
                RecordMapper.<String, String>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(String(), EmptyTemplate("item1")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(String(), EmptyTemplate("item2")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(String(), EmptyTemplate("item3")))
                        .build();
    }

    @Test
    public void shouldCreateRecordRoutingStrategy() {
        RecordRoutingStrategy strategy =
                RecordRoutingStrategy.fromSubscribedItems(SubscribedItems.create());
        assertThat(strategy).isInstanceOf(DefaultRoutingStrategy.class);
    }

    @Test
    public void shouldDefaultStrategyRoutesToSubscribedItems() {
        MappedRecord mappedRecord = mapper.map(KafkaRecord(TEST_TOPIC_1, "aKey", "aValue"));
        Set<SubscribedItem> routed = defaultStrategy("item1").route(mappedRecord);

        List<String> routedNames =
                routed.stream().map(SubscribedItem::asCanonicalItemName).toList();
        assertThat(routedNames).containsExactly("item1");
    }

    private static RecordRoutingStrategy defaultStrategy(String itemName) {
        SubscribedItems subscribed = SubscribedItems.create();
        subscribed.addItem(Items.subscribedFrom(itemName, new Object()));
        return new DefaultRoutingStrategy(subscribed);
    }
}
