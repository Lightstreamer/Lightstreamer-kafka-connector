
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class RecordRoutingTest {

    private static final String TEST_TOPIC = "topic";

    static Stream<Arguments> itemArgs() {
        return Stream.of(
                arguments(
                        List.of("item"),
                        // Routeable item
                        List.of(Items.susbcribedFrom("item", "handle1")),
                        // Non-routeable item
                        List.of(Items.susbcribedFrom("otherItem", "handle2"))));
    }

    @ParameterizedTest
    @MethodSource("itemArgs")
    public void shouldRoutesFromSimpleItems(
            List<String> templateStr,
            List<SubscribedItem> routables,
            List<SubscribedItem> nonRoutables)
            throws ExtractionException {
        ItemTemplates<String, String> templates =
                ItemTemplatesUtils.mkSimpleItems(
                        TEST_TOPIC,
                        TestSelectorSuppliers.String(),
                        templateStr.toArray(size -> new String[size]));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", "value"));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<SubscribedItem> routed = mapped.route(all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }

    static Stream<Arguments> templateArgs() {
        return Stream.of(
                arguments(
                        List.of("item-#{key=KEY,value=VALUE}"),
                        List.of(
                                Items.susbcribedFrom("item-[key=key,value=value]", "handle1"),
                                Items.susbcribedFrom("item-[value=value,key=key]", "handle2")),
                        List.of(
                                Items.susbcribedFrom("item", "handle3"),
                                Items.susbcribedFrom("item-[key=key]", "handle4"),
                                Items.susbcribedFrom("item-[key=anotherKey]", "handle5"),
                                Items.susbcribedFrom("item-[value=anotherValue]", "handle6"),
                                Items.susbcribedFrom("nonRoutable", new Object()))),
                arguments(
                        List.of(
                                "item-#{key=KEY,value=VALUE}",
                                "item-#{topic=TOPIC}",
                                "myItem-#{topic=TOPIC}"),
                        List.of(
                                Items.susbcribedFrom("item-[key=key,value=value]", "handle1"),
                                Items.susbcribedFrom("item-[value=value,key=key]", "handle2"),
                                Items.susbcribedFrom("item-[topic=topic]", "handle3"),
                                Items.susbcribedFrom("myItem-[topic=topic]", "handle4")),
                        List.of(
                                Items.susbcribedFrom("nonRoutable", "handle5"),
                                Items.susbcribedFrom("item-[key=anotherKey]", "handle6"),
                                Items.susbcribedFrom("item-[value=anotherValue]", "handle7"),
                                Items.susbcribedFrom("item-[topic=anotherTopic]", "handle8"),
                                Items.susbcribedFrom("item", "handle9"),
                                Items.susbcribedFrom("item-[key=key]", "handle10"),
                                Items.susbcribedFrom("item-[value=value]", "handle11"),
                                Items.susbcribedFrom("myItem-[topic=anotherTopic]", "handle12"))));
    }

    @ParameterizedTest
    @MethodSource("templateArgs")
    public void shouldRoutesFromTemplates(
            List<String> templateStr,
            List<SubscribedItem> routables,
            List<SubscribedItem> nonRoutables)
            throws ExtractionException {
        KeyValueSelectorSuppliers<String, String> sSuppliers = TestSelectorSuppliers.String();

        ItemTemplates<String, String> templates =
                ItemTemplatesUtils.ItemTemplates(
                        TEST_TOPIC, sSuppliers, templateStr.toArray(size -> new String[size]));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", "value"));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<SubscribedItem> routed = mapped.route(all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }

    static Stream<Arguments> templateArgsJson() {
        return Stream.of(
                arguments(
                        """
                            {
                            "name": "James",
                            "surname": "Kirk",
                            "age": 37
                            }
                            """,
                        List.of("user-#{firstName=VALUE.name,lastName=VALUE.surname}"),
                        List.of(
                                Items.susbcribedFrom(
                                        "user-[firstName=James,lastName=Kirk]", new Object())),
                        List.of(
                                Items.susbcribedFrom("item", new Object()),
                                Items.susbcribedFrom("item-[key=key]", new Object()),
                                Items.susbcribedFrom("item-[key=anotherKey]", new Object()),
                                Items.susbcribedFrom("item-[value=anotherValue]", new Object())),
                        Items.susbcribedFrom("nonRoutable", new Object())));
    }

    @ParameterizedTest
    @MethodSource("templateArgsJson")
    public void shouldRoutesFromTemplateWithJsonValueRecord(
            String jsonString,
            List<String> templateStr,
            List<SubscribedItem> routables,
            List<SubscribedItem> nonRoutables)
            throws JsonMappingException, JsonProcessingException, ExtractionException {
        ItemTemplates<String, JsonNode> templates =
                ItemTemplatesUtils.ItemTemplates(
                        TEST_TOPIC, JsonValue(), templateStr.toArray(size -> new String[size]));
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        ObjectMapper om = new ObjectMapper();
        JsonNode jsonNode = om.readTree(jsonString);
        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", jsonNode));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();
        Set<SubscribedItem> routed = mapped.route(all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }

    @ParameterizedTest
    @CsvFileSource(
            files = "src/test/resources/should-route-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldRoute(
            String template, String subscribingItem, boolean canSubscribe, boolean routeable)
            throws ExtractionException {
        ItemTemplates<GenericRecord, GenericRecord> templates =
                ItemTemplatesUtils.AvroAvroTemplates(TEST_TOPIC, template);
        RecordMapper<GenericRecord, GenericRecord> mapper =
                RecordMapper.<GenericRecord, GenericRecord>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        // .withFieldExtractor(FieldConfigs.from(Map.of()))
                        .build();

        KafkaRecord<GenericRecord, GenericRecord> incomingRecord =
                record(TEST_TOPIC, GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        SubscribedItem subscribedItem = Items.susbcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);
        Set<SubscribedItem> routed = mapped.route(Set.of(subscribedItem));
        if (routeable) {
            assertThat(routed).containsExactly(subscribedItem);
        } else {
            assertThat(routed).isEmpty();
        }

        // Stream<Item> expandedItem = templates.expand(mapped);
        // List<Item> list = expandedItem.toList();
        // assertThat(list.size()).isEqualTo(1);
        // Item first = list.get(0);
        // assertThat(first.matches(subscribedItem)).isEqualTo(exandable);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(
            files = "src/test/resources/should-expand-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldExpandMixedKeyAndValueTypes(
            String template, String subscribingItem, boolean canSubscribe, boolean exandable)
            throws ExtractionException {
        ItemTemplates<GenericRecord, JsonNode> templates =
                ItemTemplatesUtils.AvroJsonTemplates(TEST_TOPIC, template);
        RecordMapper<GenericRecord, JsonNode> mapper =
                RecordMapper.<GenericRecord, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        KafkaRecord<GenericRecord, JsonNode> incomingRecord =
                record(TEST_TOPIC, GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        SubscribedItem subscribedItem = Items.susbcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);
        Set<SubscribedItem> routed = mapped.route(Set.of(subscribedItem));
        if (exandable) {
            assertThat(routed).containsExactly(subscribedItem);
        } else {
            assertThat(routed).isEmpty();
        }

        // Stream<Item> expandedItem = templates.expand(mapped);
        // Optional<Item> first = expandedItem.findFirst();

        // assertThat(first.isPresent()).isTrue();
        // assertThat(first.get().matches(subscribedItem)).isEqualTo(exandable);
    }

    @Test
    void shouldNotRouteFromNonRoutableRecord() {}
}
