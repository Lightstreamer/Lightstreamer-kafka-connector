
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
import static com.google.common.truth.Truth8.assertThat;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.avro;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.avroKeyJsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.jsonValue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ItemTemplatesTest {

    private static final String TEST_TOPIC = "topic";

    private static <K, V> ItemTemplates<K, V> mkItemTemplates(
            SelectorSuppliers<K, V> sSuppliers, String... template) throws ExtractionException {

        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        Map<String, String> templates = new HashMap<>();
        for (int i = 0; i < template.length; i++) {
            // Add a new item template with name "template-<i>"
            templates.put("template-" + i, template[i]);
            // Add a new TopicMapping referencing the template
            topicMappings.add(TopicMappingConfig.from(TEST_TOPIC, "item-template.template-" + i));
        }
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.from(templates), topicMappings);
        return Items.from(topicsConfig, sSuppliers);
    }

    private static <K, V> ItemTemplates<K, V> mkSimpleItems(
            SelectorSuppliers<K, V> sSuppliers, String... items) throws ExtractionException {
        List<TopicMappingConfig> topicMappings = new ArrayList<>();
        for (int i = 0; i < items.length; i++) {
            // Add a new TopicMapping referencing the item name
            topicMappings.add(TopicMappingConfig.from(TEST_TOPIC, items[i]));
        }

        // No templates required in this case
        ItemTemplateConfigs noTemplatesMap = ItemTemplateConfigs.empty();

        TopicConfigurations topicsConfig = TopicConfigurations.of(noTemplatesMap, topicMappings);
        return Items.from(topicsConfig, sSuppliers);
    }

    private static ItemTemplates<GenericRecord, GenericRecord> getAvroAvroTemplates(String template)
            throws ExtractionException {
        return mkItemTemplates(avro(avroAvroConfig()), template);
    }

    private static ItemTemplates<GenericRecord, JsonNode> getAvroJsonTemplates(String template)
            throws ExtractionException {
        return mkItemTemplates(avroKeyJsonValue(avroJsonConfig()), template);
    }

    private static ConnectorConfig avroJsonConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH, "value.avsc"));
    }

    private static ConnectorConfig avroAvroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc"));
    }

    @Test
    public void shouldOneToManyTemplates() throws ExtractionException {
        SelectorSuppliers<Object, Object> sSuppliers = TestSelectorSuppliers.object();
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(Map.of("template", "stock-#{index=KEY}"));

        TopicMappingConfig topicMapping =
                TopicMappingConfig.from("stock", "item-template.template");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(topicMapping));
        ItemTemplates<Object, Object> templates = Items.from(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly("stock");

        Item subcribingItem1 = Items.subscribedFrom("stock-[index=1]");
        assertThat(templates.matches(subcribingItem1)).isTrue();
    }

    @Test
    public void shouldOneToManySimple() throws ExtractionException {
        SelectorSuppliers<String, JsonNode> sSuppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One topic mapping two items.
        TopicMappingConfig tm =
                TopicMappingConfig.from(TEST_TOPIC, "simple-item-1", "simple-item-2");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.from(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(TEST_TOPIC);

        SubscribedItem item1 = Items.susbcribedFrom("simple-item-1", "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 = Items.susbcribedFrom("simple-item-2", "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                record(TEST_TOPIC, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // Kafka records coming from same topic "topic" matches two different items
        Set<Item> expandedItems = templates.expand(mappedRecord).collect(Collectors.toSet());
        assertThat(expandedItems)
                .containsExactly(
                        Items.from("simple-item-1", Collections.emptyMap()),
                        Items.from("simple-item-2", Collections.emptyMap()));
    }

    @Test
    public void shouldOneToMany() throws ExtractionException {
        SelectorSuppliers<String, JsonNode> sSuppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One topic mapping two item templates.
        TopicMappingConfig tm =
                TopicMappingConfig.from(
                        TEST_TOPIC, "item-template.family", "item-template.relatives");
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of(
                                "family",
                                "template-family-#{topic=TOPIC,info=PARTITION}",
                                "relatives",
                                "template-relatives-#{topic=TOPIC,info1=TIMESTAMP}"));

        TopicConfigurations topicsConfig = TopicConfigurations.of(templateConfigs, List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.from(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(TEST_TOPIC);

        SubscribedItem item1 =
                Items.susbcribedFrom(
                        "template-family-[topic=aSpecificTopic,info=aSpecificPartition]",
                        "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 =
                Items.susbcribedFrom(
                        "template-relatives-[topic=anotherSpecificTopic,info1=aSpecificTimestamp]",
                        "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                record(TEST_TOPIC, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // Kafka records coming from same topic "topic" matches two different item
        // templates.
        Set<Item> expandedItems = templates.expand(mappedRecord).collect(Collectors.toSet());
        assertThat(expandedItems)
                .containsExactly(
                        Items.from("template-family", Map.of(TEST_TOPIC, "topic", "info", "150")),
                        Items.from(
                                "template-relatives", Map.of(TEST_TOPIC, "topic", "info1", "-1")));
    }

    @Test
    public void shouldManyToOneSimple() throws ExtractionException {
        SelectorSuppliers<String, JsonNode> sSuppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One item.
        String item = "orders";

        // Two topics mapping the item.
        TopicMappingConfig orderMapping = TopicMappingConfig.from("new_orders", item);
        TopicMappingConfig pastOrderMapping = TopicMappingConfig.from("past_orders", item);
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(), List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.from(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly("new_orders", "past_orders");

        SubscribedItem subcribingItem = Items.susbcribedFrom("orders", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        // Kafka Record coming from topic "new_orders"
        KafkaRecord<String, JsonNode> kafkaRecord1 =
                record("new_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);

        // Kafka Record coming from topic "past_orders"
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                record("past_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);

        // Kafka records coming from different topics ("new_orders" and "past_orders")
        // match the same item.
        Stream<Item> expandedItems1 = templates.expand(mappedRecord1);
        assertThat(expandedItems1).containsExactly(Items.from("orders", Collections.emptyMap()));

        Stream<Item> expandedItems2 = templates.expand(mappedRecord2);
        assertThat(expandedItems2).containsExactly(Items.from("orders", Collections.emptyMap()));
    }

    @Test
    public void shouldManyToOne() throws ExtractionException {
        SelectorSuppliers<String, JsonNode> sSuppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One template.
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of("template-order", "template-orders-#{topic=TOPIC}"));
        // ItemReference orders = ItemReference.forTemplate("", "template-orders-#{topic=TOPIC}");

        // Two topics mapping the template.
        TopicMappingConfig orderMapping =
                TopicMappingConfig.from("new_orders", "item-template.template-order");
        TopicMappingConfig pastOrderMapping =
                TopicMappingConfig.from("past_orders", "item-template.template-order");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.from(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly("new_orders", "past_orders");

        SubscribedItem subcribingItem =
                Items.susbcribedFrom("template-orders-[topic=aSpecifgicTopic]", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        // Kafka Record coming from topic "new_orders"
        KafkaRecord<String, JsonNode> kafkaRecord1 =
                record("new_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);

        // Kafka Record coming from topic "past_orders"
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                record("past_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);

        // Kafka records coming from different topics ("new_orders" and "past_orders")
        // match the same item template.
        Stream<Item> expandedItems1 = templates.expand(mappedRecord1);
        assertThat(expandedItems1)
                .containsExactly(Items.from("template-orders", Map.of(TEST_TOPIC, "new_orders")));

        Stream<Item> expandedItems2 = templates.expand(mappedRecord2);
        assertThat(expandedItems2)
                .containsExactly(Items.from("template-orders", Map.of(TEST_TOPIC, "past_orders")));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(
            files = "src/test/resources/should-expand-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldExpand(
            String template, String subscribingItem, boolean canSubscribe, boolean exandable)
            throws ExtractionException {
        ItemTemplates<GenericRecord, GenericRecord> templates = getAvroAvroTemplates(template);
        RecordMapper<GenericRecord, GenericRecord> mapper =
                RecordMapper.<GenericRecord, GenericRecord>builder()
                        .withExtractor(templates.extractors())
                        .build();

        KafkaRecord<GenericRecord, GenericRecord> incomingRecord =
                record(TEST_TOPIC, GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.susbcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);

        Stream<Item> expandedItem = templates.expand(mapped);
        List<Item> list = expandedItem.toList();
        assertThat(list.size()).isEqualTo(1);
        Item first = list.get(0);

        assertThat(first.matches(subscribedItem)).isEqualTo(exandable);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(
            files = "src/test/resources/should-expand-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldExpandMixedKeyAndValueTypes(
            String template, String subscribingItem, boolean canSubscribe, boolean exandable)
            throws ExtractionException {
        ItemTemplates<GenericRecord, JsonNode> templates = getAvroJsonTemplates(template);
        RecordMapper<GenericRecord, JsonNode> mapper =
                RecordMapper.<GenericRecord, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        KafkaRecord<GenericRecord, JsonNode> incomingRecord =
                record(TEST_TOPIC, GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.susbcribedFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);

        Stream<Item> expandedItem = templates.expand(mapped);
        Optional<Item> first = expandedItem.findFirst();

        assertThat(first.isPresent()).isTrue();
        assertThat(first.get().matches(subscribedItem)).isEqualTo(exandable);
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
        ItemTemplates<String, String> templates =
                mkItemTemplates(
                        TestSelectorSuppliers.string(),
                        templateStr.toArray(size -> new String[size]));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withExtractor(templates.extractors())
                        .build();

        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", "value"));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<SubscribedItem> routed = templates.routes(mapped, all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }

    static Stream<Arguments> itemArgs() {
        return Stream.of(
                arguments(
                        List.of("item"),
                        List.of(Items.susbcribedFrom("item", "handle1")),
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
                mkSimpleItems(
                        TestSelectorSuppliers.string(),
                        templateStr.toArray(size -> new String[size]));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder()
                        .withExtractor(templates.extractors())
                        .build();

        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", "value"));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<SubscribedItem> routed = templates.routes(mapped, all);
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
    public void shouldRoutesFromMoreTemplatesJson(
            String jsonString,
            List<String> templateStr,
            List<SubscribedItem> routables,
            List<SubscribedItem> nonRoutables)
            throws JsonMappingException, JsonProcessingException, ExtractionException {

        Map<String, String> updatedConfigs =
                Map.of(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, EvaluatorType.JSON.toString());
        ItemTemplates<String, JsonNode> templates =
                mkItemTemplates(
                        TestSelectorSuppliers.jsonValue(
                                ConnectorConfigProvider.minimalWith(updatedConfigs)),
                        templateStr.toArray(size -> new String[size]));
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withExtractor(templates.extractors())
                        .build();

        ObjectMapper om = new ObjectMapper();
        JsonNode jsonNode = om.readTree(jsonString);
        MappedRecord mapped = mapper.map(ConsumerRecords.record(TEST_TOPIC, "key", jsonNode));
        List<SubscribedItem> all =
                Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<SubscribedItem> routed = templates.routes(mapped, all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }

    @Test
    void shouldFailDueToInvalidTemplateException() {
        var templateConfigs = ItemTemplateConfigs.from(Map.of("template", "template-#{name=FOO}"));
        var topicMappingConfigs =
                List.of(TopicMappingConfig.from("topic", "item-template.template"));

        assertThrows(
                ExtractionException.class,
                () ->
                        Items.from(
                                TopicConfigurations.of(templateConfigs, topicMappingConfigs),
                                TestSelectorSuppliers.string()));
    }
}
