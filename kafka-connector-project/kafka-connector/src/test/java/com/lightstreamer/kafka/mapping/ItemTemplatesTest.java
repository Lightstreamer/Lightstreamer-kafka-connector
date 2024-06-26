
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

package com.lightstreamer.kafka.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;
import static com.lightstreamer.kafka.test_utils.SelectedSuppplier.avro;
import static com.lightstreamer.kafka.test_utils.SelectedSuppplier.avroKeyJsonValue;
import static com.lightstreamer.kafka.test_utils.SelectedSuppplier.jsonValue;
import static com.lightstreamer.kafka.test_utils.SelectedSuppplier.string;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.config.TopicsConfig;
import com.lightstreamer.kafka.config.TopicsConfig.ItemReference;
import com.lightstreamer.kafka.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka.mapping.Items.Item;
import com.lightstreamer.kafka.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.Selectors.Selected;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.SelectedSuppplier;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class ItemTemplatesTest {

    private static <K, V> ItemTemplates<K, V> mkItemTemplates(
            Selected<K, V> selected, String... template) {
        TopicConfiguration[] topicsConfigurations =
                Stream.of(template)
                        .map(
                                t ->
                                        new TopicConfiguration(
                                                "topic",
                                                ItemReference.forTemplate("item-template", t)))
                        .toArray(s -> new TopicConfiguration[s]);
        TopicsConfig topicsConfig = TopicsConfig.of(topicsConfigurations);
        return Items.templatesFrom(topicsConfig, selected);
    }

    private static ItemTemplates<GenericRecord, GenericRecord> getAvroAvroTemplates(
            String template) {
        return mkItemTemplates(avro(avroAvroConfig()), template);
    }

    private static ItemTemplates<GenericRecord, JsonNode> getAvroJsonTemplates(String template) {
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
    public void shouldNotAllowDuplicatedKeysOnTheSameTemplate() {
        ExpressionException e =
                assertThrows(
                        ExpressionException.class,
                        () -> mkItemTemplates(string(), "item-#{name=VALUE,name=PARTITION}"));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Found the invalid expression [item-#{name=VALUE,name=PARTITION}] while"
                                + " evaluating [item-template]: <No duplicated keys are allowed>");
    }

    @ParameterizedTest
    @ValueSource(strings = {"item-first", "item_123_", "item-", "prefix-#{}"})
    public void shouldNotAllowInvalidTemplateExpression(String templateExpression) {
        ExpressionException e =
                assertThrows(
                        ExpressionException.class,
                        () -> mkItemTemplates(string(), templateExpression));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Found the invalid expression ["
                                + templateExpression
                                + "] while"
                                + " evaluating [item-template]: <Invalid template expression>");
    }

    @Test
    public void shouldOneToManyT() {
        Selected<Object, Object> selected = SelectedSuppplier.object();
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        new TopicConfiguration(
                                "stock", ItemReference.forTemplate("", "stock-#{index=KEY}")));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, selected);
        assertThat(templates.topics()).containsExactly("stock");

        Item subcribingItem1 = Items.itemFrom("stock-[index=1]");
        assertThat(templates.matches(subcribingItem1)).isTrue();
    }

    @Test
    public void shouldOneToMany() {
        Selected<String, JsonNode> selected = jsonValue(ConnectorConfigProvider.minimal());

        // One topic mapping two templates.
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        new TopicConfiguration(
                                "topic",
                                ItemReference.forTemplate(
                                        "", "template-family-#{topic=TOPIC,info=PARTITION}")),
                        new TopicConfiguration(
                                "topic",
                                ItemReference.forTemplate(
                                        "", "template-relatives-#{topic=TOPIC,info=TIMESTAMP}")));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, selected);
        assertThat(templates.topics()).containsExactly("topic");

        Item subcribingItem1 =
                Items.itemFrom(
                        "template-family-[topic=aSpecificTopic,info=aSpecificPartition]", "");
        assertThat(templates.matches(subcribingItem1)).isTrue();

        Item subcribingItem2 =
                Items.itemFrom(
                        "template-relatives-[topic=anotherSpecificTopic,info=aSpecificTimestamp]",
                        "");
        assertThat(templates.matches(subcribingItem2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord = record("topic", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // Kafka records coming from same topic "topic" matches two different item
        // templates.
        Stream<Item> expandedItems = templates.expand(mappedRecord);
        assertThat(expandedItems)
                .containsExactly(
                        Items.itemFrom(
                                "", "template-family", Map.of("topic", "topic", "info", "150")),
                        Items.itemFrom(
                                "", "template-relatives", Map.of("topic", "topic", "info", "-1")));
    }

    @Test
    public void shouldManyToOne() {
        Selected<String, JsonNode> suppliers = jsonValue(ConnectorConfigProvider.minimal());

        // One template.
        ItemReference ordersTemplate =
                ItemReference.forTemplate("", "template-orders-#{topic=TOPIC}");

        // Two topics mapping the template.
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        new TopicConfiguration("new_orders", ordersTemplate),
                        new TopicConfiguration("past_orders", ordersTemplate));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, suppliers);
        assertThat(templates.topics()).containsExactly("new_orders", "past_orders");

        Item subcribingItem = Items.itemFrom("template-orders-[topic=aSpecifgicTopic]", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        // Kafka Record coming from topic "new_orders"
        KafkaRecord<String, JsonNode> kafkaRecord1 =
                record("new_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);

        // Kafka Record coming from tpoic "past_orders"
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                record("past_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);

        // Kafka records coming from different topics ("new_orders" and "past_orders")
        // match the same item template.
        Stream<Item> expandedItems1 = templates.expand(mappedRecord1);
        assertThat(expandedItems1)
                .containsExactly(
                        Items.itemFrom("", "template-orders", Map.of("topic", "new_orders")));

        Stream<Item> expandedItems2 = templates.expand(mappedRecord2);
        assertThat(expandedItems2)
                .containsExactly(
                        Items.itemFrom("", "template-orders", Map.of("topic", "past_orders")));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(
            files = "src/test/resources/should-expand-items.csv",
            useHeadersInDisplayName = true,
            delimiter = '|')
    public void shouldExpand(
            String template, String subscribingItem, boolean canSubscribe, boolean exandable) {
        ItemTemplates<GenericRecord, GenericRecord> templates = getAvroAvroTemplates(template);
        RecordMapper<GenericRecord, GenericRecord> mapper =
                RecordMapper.<GenericRecord, GenericRecord>builder()
                        .withSelectors(templates.selectors())
                        .build();

        KafkaRecord<GenericRecord, GenericRecord> incomingRecord =
                record("topic", GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.itemFrom(subscribingItem, new Object());

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
            String template, String subscribingItem, boolean canSubscribe, boolean exandable) {
        ItemTemplates<GenericRecord, JsonNode> templates = getAvroJsonTemplates(template);
        RecordMapper<GenericRecord, JsonNode> mapper =
                RecordMapper.<GenericRecord, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        KafkaRecord<GenericRecord, JsonNode> incomingRecord =
                record("topic", GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.itemFrom(subscribingItem, new Object());

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
                                Items.itemFrom("item-[key=key,value=value]", new Object()),
                                Items.itemFrom("item-[value=value,key=key]", new Object())),
                        List.of(
                                Items.itemFrom("item", new Object()),
                                Items.itemFrom("item-[key=key]", new Object()),
                                Items.itemFrom("item-[key=anotherKey]", new Object()),
                                Items.itemFrom("item-[value=anotherValue]", new Object())),
                        Items.itemFrom("nonRoutable", new Object())),
                arguments(
                        List.of(
                                "item-#{key=KEY,value=VALUE}",
                                "item-#{topic=TOPIC}",
                                "myItem-#{topic=TOPIC}"),
                        List.of(
                                Items.itemFrom("item-[key=key,value=value]", new Object()),
                                Items.itemFrom("item-[value=value,key=key]", new Object()),
                                Items.itemFrom("item-[topic=topic]", new Object()),
                                Items.itemFrom("myItem-[topic=topic]", new Object())),
                        List.of(
                                Items.itemFrom("nonRoutable", new Object()),
                                Items.itemFrom("item-[key=anotherKey]", new Object()),
                                Items.itemFrom("item-[value=anotherValue]", new Object()),
                                Items.itemFrom("item-[topic=anotherTopic]", new Object()),
                                Items.itemFrom("item", new Object()),
                                Items.itemFrom("item-[key=key]", new Object()),
                                Items.itemFrom("item-[value=value]", new Object()),
                                Items.itemFrom("myItem-[topic=anotherTopic]", new Object()))));
    }

    @ParameterizedTest
    @MethodSource("templateArgs")
    public void shouldRoutesFromMoreTemplates(
            List<String> templateStr, List<Item> routables, List<Item> nonRoutables) {
        ItemTemplates<String, String> templates =
                mkItemTemplates(
                        SelectedSuppplier.string(), templateStr.toArray(size -> new String[size]));
        RecordMapper<String, String> mapper =
                RecordMapper.<String, String>builder().withSelectors(templates.selectors()).build();

        MappedRecord mapped = mapper.map(ConsumerRecords.record("topic", "key", "value"));
        List<Item> all = Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<Item> routed = templates.routes(mapped, all);
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
                                Items.itemFrom(
                                        "user-[firstName=James,lastName=Kirk]", new Object())),
                        List.of(
                                Items.itemFrom("item", new Object()),
                                Items.itemFrom("item-[key=key]", new Object()),
                                Items.itemFrom("item-[key=anotherKey]", new Object()),
                                Items.itemFrom("item-[value=anotherValue]", new Object())),
                        Items.itemFrom("nonRoutable", new Object())));
    }

    @ParameterizedTest
    @MethodSource("templateArgsJson")
    public void shouldRoutesFromMoreTemplatesJson(
            String jsonString,
            List<String> templateStr,
            List<Item> routables,
            List<Item> nonRoutables)
            throws JsonMappingException, JsonProcessingException {

        Map<String, String> updatedConfigs =
                Map.of(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, EvaluatorType.JSON.toString());
        ItemTemplates<String, JsonNode> templates =
                mkItemTemplates(
                        SelectedSuppplier.jsonValue(
                                ConnectorConfigProvider.minimalWith(updatedConfigs)),
                        templateStr.toArray(size -> new String[size]));
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        ObjectMapper om = new ObjectMapper();
        JsonNode jsonNode = om.readTree(jsonString);
        MappedRecord mapped = mapper.map(ConsumerRecords.record("topic", "key", jsonNode));
        List<Item> all = Stream.concat(routables.stream(), nonRoutables.stream()).toList();

        Set<Item> routed = templates.routes(mapped, all);
        assertThat(routed).containsExactlyElementsIn(routables);
    }
}
