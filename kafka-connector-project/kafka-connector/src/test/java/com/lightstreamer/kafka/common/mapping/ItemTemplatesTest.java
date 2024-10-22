
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
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Object;

import static java.util.Collections.emptySet;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ItemTemplatesTest {

    private static final String TEST_TOPIC = "topic";

    @Test
    public void shouldCreateOneToOneItemTemplateFromSimpleItem() throws ExtractionException {
        // One topic mapping one item
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(TEST_TOPIC, "simple-item-1");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(tm));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC);
        assertThat(templates.extractorsByTopicName()).hasSize(1);
        assertThat(templates.extractorsByTopicName().get(TEST_TOPIC)).hasSize(1);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC))
                .containsExactly(Schema.from("simple-item-1", emptySet()));
    }

    @Test
    public void shouldCreateOneToOneItemTemplate() throws ExtractionException {
        // One topic mapping one item template
        TopicMappingConfig topicMapping =
                TopicMappingConfig.fromDelimitedMappings("stocks", "item-template.template");
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(Map.of("template", "stock-#{index=KEY.attrib}"));
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(topicMapping));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.topics()).containsExactly("stocks");
        assertThat(templates.extractorsByTopicName()).hasSize(1);
        assertThat(templates.extractorsByTopicName().get("stocks")).hasSize(1);
        assertThat(templates.getExtractorSchemasByTopicName("stocks"))
                .containsExactly(Schema.from("stock", Set.of("index")));

        Item subcribingItem1 = Items.subscribedFrom("stock-[index=1]");
        assertThat(templates.matches(subcribingItem1)).isTrue();
    }

    @Test
    public void shouldCreateOneToManyItemTemplateFromSimpleItem() throws ExtractionException {
        // One topic mapping two items.
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(TEST_TOPIC, "simple-item-1,simple-item-2");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, JsonValue());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC);
        assertThat(templates.extractorsByTopicName()).hasSize(1);
        assertThat(templates.extractorsByTopicName().get(TEST_TOPIC)).hasSize(2);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC))
                .containsExactly(
                        Schema.from("simple-item-1", emptySet()),
                        Schema.from("simple-item-2", emptySet()));

        SubscribedItem item1 = Items.susbcribedFrom("simple-item-1", "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 = Items.susbcribedFrom("simple-item-2", "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();

        SubscribedItem item3 = Items.susbcribedFrom("simple-item-3", "itemHandle2");
        // assertThat(templates.matches(item2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                record(TEST_TOPIC, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // Kafka records coming from same topic "topic" matches two different items
        // Set<Item> expandedItems = templates.expand(mappedRecord).collect(Collectors.toSet());
        Set<SubscribedItem> expandedItems = mappedRecord.route(Set.of(item1, item2, item3));
        assertThat(expandedItems).containsExactly(item1, item2);
    }

    @Test
    public void shouldCreateOneToManyItemTemplate() throws ExtractionException {
        // One topic mapping two item templates.
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC, "item-template.family,item-template.relatives");
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of(
                                "family",
                                "template-family-#{topic=TOPIC,info=PARTITION}",
                                "relatives",
                                "template-relatives-#{topic=TOPIC,info1=TIMESTAMP}"));

        TopicConfigurations topicsConfig = TopicConfigurations.of(templateConfigs, List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, JsonValue());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC);
        assertThat(templates.extractorsByTopicName()).hasSize(1);
        assertThat(templates.extractorsByTopicName().get(TEST_TOPIC)).hasSize(2);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC))
                .containsExactly(
                        Schema.from("template-family", Set.of("topic", "info")),
                        Schema.from("template-relatives", Set.of("topic", "info1")));

        SubscribedItem item1 =
                Items.susbcribedFrom(
                        "template-family-[topic=" + TEST_TOPIC + ",info=150]", "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 =
                Items.susbcribedFrom(
                        "template-relatives-[topic=" + TEST_TOPIC + ",info1=-1]", "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                record(TEST_TOPIC, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // Kafka records coming from same topic "topic" matches two different item
        // templates.
        // Set<Item> expandedItems = templates.expand(mappedRecord).collect(Collectors.toSet());
        // assertThat(expandedItems)
        //         .containsExactly(
        //         Items.from("template-family", Map.of(TEST_TOPIC, "topic", "info", "150")),
        //         Items.from(
        //                 "template-relatives", Map.of(TEST_TOPIC, "topic", "info1", "-1")));
        Set<SubscribedItem> routed = mappedRecord.route(Set.of(item1, item2));
        assertThat(routed).containsExactly(item1, item2);
    }

    @Test
    public void shouldCreateManyToOneItemTemplateFromSimpleItem() throws ExtractionException {
        KeyValueSelectorSuppliers<String, JsonNode> sSuppliers = JsonValue();

        // One item.
        String item = "orders";

        // Two topics mapping the item.
        String newOrdersTopic = "new_orders";
        String pastOrderTopic = "past_orders";
        TopicMappingConfig orderMapping =
                TopicMappingConfig.fromDelimitedMappings(newOrdersTopic, item);
        TopicMappingConfig pastOrderMapping =
                TopicMappingConfig.fromDelimitedMappings(pastOrderTopic, item);
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(), List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(newOrdersTopic, pastOrderTopic);

        SubscribedItem subcribingItem = Items.susbcribedFrom("orders", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        // Kafka Record coming from topic "new_orders"
        KafkaRecord<String, JsonNode> kafkaRecord1 =
                record(newOrdersTopic, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        assertThat(mappedRecord1.route(Set.of(subcribingItem))).containsExactly(subcribingItem);

        // Kafka Record coming from topic "past_orders"
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                record(pastOrderTopic, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        assertThat(mappedRecord2.route(Set.of(subcribingItem))).containsExactly(subcribingItem);

        // Kafka records coming from different topics ("new_orders" and "past_orders")
        // match the same item.
        // Stream<Item> expandedItems1 = templates.expand(mappedRecord1);
        // assertThat(expandedItems1).containsExactly(Items.from("orders", Collections.emptyMap()));

        // Stream<Item> expandedItems2 = templates.expand(mappedRecord2);
        // assertThat(expandedItems2).containsExactly(Items.from("orders", Collections.emptyMap()));

    }

    @Test
    public void shouldManyToOne() throws ExtractionException {
        KeyValueSelectorSuppliers<String, JsonNode> sSuppliers = JsonValue();

        // One template.
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of("template-order", "template-orders-#{topic=TOPIC}"));
        // ItemReference orders = ItemReference.forTemplate("", "template-orders-#{topic=TOPIC}");

        // Two topics mapping the template.
        String newOrdersTopic = "new_orders";
        String pastOrderTopic = "past_orders";
        TopicMappingConfig orderMapping =
                TopicMappingConfig.fromDelimitedMappings(
                        newOrdersTopic, "item-template.template-order");
        TopicMappingConfig pastOrderMapping =
                TopicMappingConfig.fromDelimitedMappings(
                        pastOrderTopic, "item-template.template-order");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(newOrdersTopic, pastOrderTopic);

        SubscribedItem itemFilteringTopic1 =
                Items.susbcribedFrom("template-orders-[topic=new_orders]", "");
        assertThat(templates.matches(itemFilteringTopic1)).isTrue();

        SubscribedItem itemFilteringTopic2 =
                Items.susbcribedFrom("template-orders-[topic=past_orders]", "");
        assertThat(templates.matches(itemFilteringTopic1)).isTrue();

        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractors(templates.extractorsByTopicName())
                        .build();

        // Kafka Record coming from topic "new_orders"
        KafkaRecord<String, JsonNode> kafkaRecord1 =
                record(newOrdersTopic, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        assertThat(mappedRecord1.route(Set.of(itemFilteringTopic1, itemFilteringTopic2)))
                .containsExactly(itemFilteringTopic1);

        // Kafka Record coming from topic "past_orders"
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                record(pastOrderTopic, "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        assertThat(mappedRecord2.route(Set.of(itemFilteringTopic1, itemFilteringTopic2)))
                .containsExactly(itemFilteringTopic2);

        // Kafka records coming from different topics ("new_orders" and "past_orders")
        // match the same item template.
        // Stream<Item> expandedItems1 = templates.expand(mappedRecord1);
        // assertThat(expandedItems1)
        //         .containsExactly(Items.from("template-orders", Map.of(TEST_TOPIC,
        // "new_orders")));

        // Stream<Item> expandedItems2 = templates.expand(mappedRecord2);
        // assertThat(expandedItems2)
        //         .containsExactly(Items.from("template-orders", Map.of(TEST_TOPIC,
        // "past_orders")));
    }
}
