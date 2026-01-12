
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
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecordWithHeaders;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleGenericRecordProvider;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RouteAllStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateRecordRoutingStrategy(boolean allowImplicitItems) {
        RecordRoutingStrategy strategy =
                RecordRoutingStrategy.fromSubscribedItems(
                        allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create());

        if (allowImplicitItems) {
            assertThat(strategy).isInstanceOf(RouteAllStrategy.class);
        } else {
            assertThat(strategy).isInstanceOf(DefaultRoutingStrategy.class);
        }
    }

    @Test
    public void shouldDefaultStrategyRoutesToSubscribedItems() {
        MappedRecord mappedRecord = mapper.map(KafkaRecord(TEST_TOPIC_1, "aKey", "aValue"));
        Set<SubscribedItem> routed = defaultStrategy("item1").route(mappedRecord);

        List<String> routedNames =
                routed.stream().map(SubscribedItem::asCanonicalItemName).toList();
        assertThat(routedNames).containsExactly("item1");
    }

    @Test
    public void shouldDefaultStrategyNotRouteToImplicitItems() {
        assertThat(defaultStrategy("item1").canRouteImplicitItems()).isFalse();
    }

    @Test
    public void shouldRouteAllStrategyRouteToAllItems() {
        MappedRecord mappedRecord = mapper.map(KafkaRecord(TEST_TOPIC_1, "aKey", "aValue"));
        Set<SubscribedItem> routed = routeAllStrategy().route(mappedRecord);

        List<String> routedNames =
                routed.stream().map(SubscribedItem::asCanonicalItemName).toList();
        assertThat(routedNames).containsExactly("item1", "item2", "item3");
    }

    @Test
    public void shouldRouteAllStrategyRouteToAllItemsComplex() throws ExtractionException {
        String template1 =
                "complex-item-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}";
        String template2 =
                "complex-item-#{pref=KEY.preferences['pref1'],childName1=VALUE.children[0].name,childName2=VALUE.children[1].name}";
        ItemTemplates<GenericRecord, JsonNode> templates =
                ItemTemplatesUtils.ItemTemplates(
                        TestSelectorSuppliers.AvroKeyJsonValue(),
                        List.of(TEST_TOPIC_1),
                        List.of(template1, template2));
        RecordMapper<GenericRecord, JsonNode> mapper =
                RecordMapper.<GenericRecord, JsonNode>builder()
                        .withCanonicalItemExtractors(templates.groupExtractors())
                        .build();

        KafkaRecord<GenericRecord, JsonNode> incomingRecord =
                KafkaRecordWithHeaders(
                        TEST_TOPIC_1,
                        SampleGenericRecordProvider().sampleMessage(),
                        SampleJsonNodeProvider().sampleMessage(),
                        new RecordHeaders()
                                .add("header-key1", "header-value1".getBytes())
                                .add("header-key2", "header-value2".getBytes()));
        MappedRecord mapped = mapper.map(incomingRecord);
        List<String> implicitItems =
                routeAllStrategy().route(mapped).stream()
                        .map(SubscribedItem::asCanonicalItemName)
                        .toList();

        assertThat(implicitItems)
                .containsExactly(
                        "complex-item-[child=alex,keyName=joe,name=joe]",
                        "complex-item-[childName1=alex,childName2=anna,pref=pref_value1]");
    }

    @Test
    public void shouldRouteAllStrategyRoutesToImplicitItems() {
        assertThat(routeAllStrategy().canRouteImplicitItems()).isTrue();
    }

    private static RecordRoutingStrategy defaultStrategy(String itemName) {
        SubscribedItems subscribed = SubscribedItems.create();
        subscribed.addItem(Items.subscribedFrom(itemName, new Object()));
        return new DefaultRoutingStrategy(subscribed);
    }

    private static RecordRoutingStrategy routeAllStrategy() {
        return new RouteAllStrategy();
    }
}
