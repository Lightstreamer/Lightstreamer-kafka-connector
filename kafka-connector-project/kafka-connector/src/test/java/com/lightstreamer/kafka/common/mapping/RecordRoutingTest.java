
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
import static com.lightstreamer.kafka.common.mapping.Items.subcribedFrom;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class RecordRoutingTest {

    private static final String TEST_TOPIC_1 = "topic";
    private static final String TEST_TOPIC_2 = "anotherTopic";

    static Stream<Arguments> itemArgs() {
        return Stream.of(
                // One-to-One
                arguments(
                        List.of(TEST_TOPIC_1),
                        "item",
                        // Routeable item
                        List.of(subcribedFrom("item", "handle1")),
                        // Non-routeable item
                        List.of(subcribedFrom("otherItem", "handle2"))),
                // Many-to-One
                arguments(
                        List.of(TEST_TOPIC_1, TEST_TOPIC_2),
                        "item",
                        // Routeable item
                        List.of(subcribedFrom("item", "handle1")),
                        // Non-routeable item
                        List.of(subcribedFrom("otherItem", "handle2"))));
    }

    @ParameterizedTest
    @MethodSource("itemArgs")
    public void shouldRoutesFromSimpleItems(
            List<String> topics,
            String item,
            List<SubscribedItem> routables,
            List<SubscribedItem> nonRoutables)
            throws ExtractionException {
        ItemTemplates<String, String> templates =
                ItemTemplatesUtils.mkSimpleItems(
                        OthersSelectorSuppliers.String(), topics, List.of(item));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        for (String topic : topics) {
            MappedRecord mapped = mapper.map(ConsumerRecords.record(topic, "key", "value"));
            List<SubscribedItem> all =
                    Stream.concat(routables.stream(), nonRoutables.stream()).toList();

            Set<SubscribedItem> routed = mapped.route(all);
            assertThat(routed).containsExactlyElementsIn(routables);
        }
    }

    static Stream<Arguments> templateArgs() {
        return Stream.of(
                arguments(
                        List.of(TEST_TOPIC_1, TEST_TOPIC_2),
                        List.of("item-#{key=KEY,value=VALUE,topic=TOPIC}"),
                        Map.of(
                                // Routable items for TEST_TOPIC_1
                                TEST_TOPIC_1,
                                List.of(
                                        subcribedFrom(
                                                "item-[key=key,value=value,topic=topic]",
                                                "handle1"),
                                        subcribedFrom(
                                                "item-[value=value,topic=topic,key=key]",
                                                "handle2")),
                                // Routable items for TEST_TOPIC_2
                                TEST_TOPIC_2,
                                List.of(
                                        subcribedFrom(
                                                "item-[key=key,value=value,topic=anotherTopic]",
                                                "handle1"),
                                        subcribedFrom(
                                                "item-[topic=anotherTopic,value=value,key=key]",
                                                "handle2"))),
                        Map.of(
                                // Non-routable items for TEST_TOPIC_1
                                TEST_TOPIC_1,
                                List.of(
                                        subcribedFrom(
                                                "item-[key=key,value=value,topic=anotherTopic]",
                                                "handle1"),
                                        subcribedFrom("item", "handle3"),
                                        subcribedFrom("item-[key=key]", "handle4"),
                                        subcribedFrom("item-[key=anotherKey]", "handle5"),
                                        subcribedFrom("item-[value=anotherValue]", "handle6"),
                                        subcribedFrom("nonRoutable", new Object())),
                                // Non-routable items for TEST_TOPIC_2
                                TEST_TOPIC_2,
                                List.of(
                                        subcribedFrom(
                                                "item-[key=key,value=value,topic=topic]",
                                                "handle1"),
                                        subcribedFrom("item", "handle3"),
                                        subcribedFrom("item-[key=key]", "handle4"),
                                        subcribedFrom("item-[key=anotherKey]", "handle5"),
                                        subcribedFrom("item-[value=anotherValue]", "handle6"),
                                        subcribedFrom("nonRoutable", new Object())))));
    }

    @ParameterizedTest
    @MethodSource("templateArgs")
    public void shouldRoutesFromTemplates(
            List<String> topics,
            List<String> templateStr,
            Map<String, List<SubscribedItem>> routables,
            Map<String, List<SubscribedItem>> nonRoutables)
            throws ExtractionException {
        ItemTemplates<String, String> templates =
                ItemTemplatesUtils.ItemTemplates(
                        OthersSelectorSuppliers.String(), topics, templateStr);
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        for (String topic : topics) {
            MappedRecord mapped = mapper.map(ConsumerRecords.record(topic, "key", "value"));
            List<SubscribedItem> routablesForTopic = routables.get(topic);
            List<SubscribedItem> nonRoutableForTopic = nonRoutables.get(topic);

            List<SubscribedItem> all = new ArrayList<>(routablesForTopic);
            all.addAll(nonRoutableForTopic);

            Set<SubscribedItem> routed = mapped.route(all);
            assertThat(routed).containsExactlyElementsIn(routablesForTopic);
        }
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
                                subcribedFrom(
                                        "user-[firstName=James,lastName=Kirk]", new Object())),
                        List.of(
                                subcribedFrom("item", new Object()),
                                subcribedFrom("item-[key=key]", new Object()),
                                subcribedFrom("item-[key=anotherKey]", new Object()),
                                subcribedFrom("item-[value=anotherValue]", new Object()),
                                subcribedFrom("nonRoutable", new Object()))));
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
                ItemTemplatesUtils.ItemTemplates(JsonValue(), List.of(TEST_TOPIC_1), templateStr);
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        ObjectMapper om = new ObjectMapper();
        JsonNode jsonNode = om.readTree(jsonString);
        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC_1, "key", jsonNode));
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
                ItemTemplatesUtils.AvroAvroTemplates(TEST_TOPIC_1, template);
        RecordMapper<GenericRecord, GenericRecord> mapper =
                RecordMapper.<GenericRecord, GenericRecord>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        KafkaRecord<GenericRecord, GenericRecord> incomingRecord =
                record(TEST_TOPIC_1, GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        SubscribedItem subscribedItem = subcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);
        Set<SubscribedItem> routed = mapped.route(Set.of(subscribedItem));
        if (routeable) {
            assertThat(routed).containsExactly(subscribedItem);
        } else {
            assertThat(routed).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(
            files = "src/test/resources/should-route-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldRouteWithMixedKeyAndValueTypes(
            String template, String subscribingItem, boolean canSubscribe, boolean routable)
            throws ExtractionException {
        ItemTemplates<GenericRecord, JsonNode> templates =
                ItemTemplatesUtils.AvroJsonTemplates(TEST_TOPIC_1, template);
        RecordMapper<GenericRecord, JsonNode> mapper =
                RecordMapper.<GenericRecord, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        KafkaRecord<GenericRecord, JsonNode> incomingRecord =
                record(TEST_TOPIC_1, GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        SubscribedItem subscribedItem = subcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);
        Set<SubscribedItem> routed = mapped.route(Set.of(subscribedItem));
        if (routable) {
            assertThat(routed).containsExactly(subscribedItem);
        } else {
            assertThat(routed).isEmpty();
        }
    }
}
