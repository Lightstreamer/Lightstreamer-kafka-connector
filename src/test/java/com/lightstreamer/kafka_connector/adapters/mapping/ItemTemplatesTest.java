
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

package com.lightstreamer.kafka_connector.adapters.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords.record;
import static com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers.avro;
import static com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers.avroKeyJsonValue;
import static com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers.jsonValue;
import static com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers.string;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.JsonNodeProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

public class ItemTemplatesTest {

    private static <K, V> ItemTemplates<K, V> templates(
            SelectorsSupplier<K, V> selectionsSupplier, String template) {
        TopicsConfig topicsConfig =
                TopicsConfig.of(new TopicConfiguration("topic", "item-template", template));
        return Items.templatesFrom(topicsConfig, selectionsSupplier);
    }

    private static ItemTemplates<GenericRecord, GenericRecord> getAvroAvroTemplates(
            String template) {
        return templates(avro(avroAvroConfig()), template);
    }

    private static ItemTemplates<GenericRecord, JsonNode> getAvroJsonTemplates(String template) {
        return templates(avroKeyJsonValue(avroJsonConfig()), template);
    }

    private static ConnectorConfig avroJsonConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources", Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc"));
    }

    private static ConnectorConfig avroAvroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        ConnectorConfig.KEY_SCHEMA_FILE,
                        "value.avsc",
                        ConnectorConfig.VALUE_SCHEMA_FILE,
                        "value.avsc"));
    }

    @Test
    public void shouldNotAllowDuplicatedKeysOnTheSameTemplate() {
        ExpressionException e =
                assertThrows(
                        ExpressionException.class,
                        () -> templates(string(), "item-#{name=VALUE,name=PARTITION}"));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Found the invalid expression [item-#{name=VALUE,name=PARTITION}] while evaluating [item-template]: <No duplicated keys are allowed>");
    }

    @Test
    public void shouldOneToMany() {
        SelectorsSupplier<String, JsonNode> suppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One topic mapping two templates.
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        new TopicConfiguration(
                                "topic", "template-family-#{topic=TOPIC,info=PARTITION}"),
                        new TopicConfiguration(
                                "topic", "template-relatives-#{topic=TOPIC,info=TIMESTAMP}"));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, suppliers);
        assertThat(templates.topics()).containsExactly("topic");

        Item subcribingItem1 = Items.itemFrom("template-family-<topic=aSpecificTopic>", "");
        assertThat(templates.matches(subcribingItem1)).isTrue();

        Item subcribingItem2 =
                Items.itemFrom("template-relatives-<topic=anotherSpecificTopic>", "");
        assertThat(templates.matches(subcribingItem2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        ConsumerRecord<String, JsonNode> kafkaRecord =
                record("topic", "key", JsonNodeProvider.RECORD);
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
        SelectorsSupplier<String, JsonNode> suppliers =
                jsonValue(ConnectorConfigProvider.minimal());

        // One template.
        String ordersTemplate = "template-orders-#{topic=TOPIC}";

        // Two topics mapping the template.
        TopicsConfig topicsConfig =
                TopicsConfig.of(
                        new TopicConfiguration("new_orders", ordersTemplate),
                        new TopicConfiguration("past_orders", ordersTemplate));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, suppliers);
        assertThat(templates.topics()).containsExactly("new_orders", "past_orders");

        Item subcribingItem = Items.itemFrom("template-orders-<topic=aSpecifgicTopic>", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withSelectors(templates.selectors())
                        .build();

        // Kafka Record coming from topic "new_orders"
        ConsumerRecord<String, JsonNode> kafkaRecord1 =
                record("new_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);

        // Kafka Record coming from tpoic "past_orders"
        ConsumerRecord<String, JsonNode> kafkaRecord2 =
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

    @Tag("integration")
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

        ConsumerRecord<GenericRecord, GenericRecord> incomingRecord =
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

    @Tag("integration")
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

        ConsumerRecord<GenericRecord, JsonNode> incomingRecord =
                record("topic", GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.itemFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);

        Stream<Item> expandedItem = templates.expand(mapped);
        Optional<Item> first = expandedItem.findFirst();

        assertThat(first.isPresent()).isTrue();
        assertThat(first.get().matches(subscribedItem)).isEqualTo(exandable);
    }
}
