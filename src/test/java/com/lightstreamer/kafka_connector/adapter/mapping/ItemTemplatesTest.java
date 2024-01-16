package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.record;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.SelectorsSuppliers;

public class ItemTemplatesTest {

    // @Tag("unit")
    // @ParameterizedTest
    // @EmptySource
    // @ValueSource(strings = { "a,", ".", "|", "@" })
    // public void shouldNotCreateDueToInvalidTemplate(String invalidTemplate) {
    // RuntimeException exception = assertThrows(RuntimeException.class,
    // () -> ItemTemplate.of("topic", invalidTemplate,
    // RecordInspector.builder()));
    // assertThat(exception.getMessage()).isEqualTo("Invalid item");
    // }

    private static ConnectorConfig config = new ConnectorConfig(Collections.emptyMap());

    private static <K, V> ItemTemplates<K, V> templates(SelectorsSupplier<K, V> selectionsSupplier,
            String... template) {
        List<TopicMapping> topicMappings = List.of(new TopicMapping("topic", Arrays.asList(template)));
        return Items.templatesFrom(topicMappings, selectionsSupplier);
    }

    private static <K, V> ItemTemplates<K, V> templates2(SelectorsSupplier<K, V> selectionsSupplier,
            List<String> topics, String... template) {
        List<TopicMapping> topicMappings = List.of(new TopicMapping("topic", Arrays.asList(template)));
        return Items.templatesFrom(topicMappings, selectionsSupplier);
    }

    private static ItemTemplates<GenericRecord, GenericRecord> getGenericRecordsGenericRecordTemplates(
            String template) {
        return templates(SelectorsSuppliers.genericRecord(), template);
    }

    private static ItemTemplates<GenericRecord, JsonNode> getGenericRecordJsonNodeTemplates(String template) {
        return templates(SelectorsSuppliers.genericRecordKeyJsonNodeValue(), template);
    }

    @Test
    public void shouldNotAllowDuplicatedKeysOnTheSameTemplat() {
        ExpressionException e = assertThrows(ExpressionException.class,
                () -> templates(SelectorsSuppliers.string(), "item-${name=VALUE,name=PARTITION}"));
        assertThat(e.getMessage()).isEqualTo("No duplicated keys are allowed");
    }

    @Test
    public void shouldOneToMany() {
        SelectorsSupplier<String, JsonNode> suppliers = SelectorsSuppliers.jsonNodeValue();

        List<TopicMapping> tp = List.of(
                new TopicMapping("topic",
                        List.of("family-${topic=TOPIC,info=PARTITION}",
                                "relatives-${topic=TOPIC,info=TIMESTAMP}")));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(tp, suppliers);

        Item subcribingItem1 = Items.itemFrom("family-<topic=topic>", "");
        assertThat(templates.matches(subcribingItem1)).isTrue();

        Item subcribingItem2 = Items.itemFrom("relatives-<topic=topic>", "");
        assertThat(templates.matches(subcribingItem2)).isTrue();

        RecordMapper<String, JsonNode> mapper = RecordMapper
                .<String, JsonNode>builder()
                .withSelectors(templates.selectors())
                .build();

        ConsumerRecord<String, JsonNode> record = record("topic", "key", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(record);

        List<Item> expandedItems = templates.expand(mappedRecord).toList();
        assertThat(expandedItems).hasSize(2);

        Map<String, String> valuesFamily = mappedRecord.filter(templates.selectorsByName("family").get());
        assertThat(valuesFamily).containsExactly("topic", "topic", "info", "150");

        // valuesFamily = mappedRecord.filter(Schema.of(SchemaName.of("family"),
        // "info"));
        // assertThat(valuesFamily).containsExactly("info", "150");

        Map<String, String> valuesRelatived = mappedRecord.filter(templates.selectorsByName("relatives").get());
        assertThat(valuesRelatived).containsExactly("topic", "topic", "info", "-1");
    }

    @Test
    public void shouldManyToOne() {
        SelectorsSupplier<String, JsonNode> suppliers = SelectorsSuppliers.jsonNodeValue();

        List<TopicMapping> tp = List.of(
                new TopicMapping("new_orders", List.of("orders-${topic=TOPIC}", "item-${topic=TOPIC}")),
                new TopicMapping("past_orders", List.of("orders-${topic=TOPIC}")));
        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(tp, suppliers);

        Item subcribingItem = Items.itemFrom("orders-<topic=new_orders>", "");
        assertThat(templates.matches(subcribingItem)).isTrue();

        RecordMapper<String, JsonNode> mapper = RecordMapper
                .<String, JsonNode>builder()
                .withSelectors(templates.selectors())
                .build();

        ConsumerRecord<String, JsonNode> record = record("new_orders", "key", JsonNodeProvider.RECORD);
        MappedRecord map = mapper.map(record);

        List<Item> expandedItems = templates.expand(map).toList();
        assertThat(expandedItems).hasSize(2);

        Map<String, String> newOrders = map.filter(templates.selectorsByName("orders").get());
        assertThat(newOrders).containsExactly("topic", "new_orders");

        // Map<String, String> pastOrders =
        // map.filter(Schema.of(SchemaName.of("past_orders"), "topic"));
        // assertThat(pastOrders).isEmpty();
    }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvFileSource(files = "src/test/resources/should-expand-items.csv", useHeadersInDisplayName = true, delimiter = '|')
    public void shouldExpand(String template, String subscribingItem, boolean canSubscribe, boolean exandable) {
        ItemTemplates<GenericRecord, GenericRecord> templates = getGenericRecordsGenericRecordTemplates(
                template);
        RecordMapper<GenericRecord, GenericRecord> mapper = RecordMapper
                .<GenericRecord, GenericRecord>builder()
                .withSelectors(templates.selectors())
                .build();

        ConsumerRecord<GenericRecord, GenericRecord> incomingRecord = record("topic",
                GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
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
    @CsvFileSource(files = "src/test/resources/should-expand-items.csv", useHeadersInDisplayName = true, delimiter = '|')
    public void shouldExpandMixedKeyAndValueTypes(String template, String subscribingItem, boolean canSubscribe,
            boolean exandable) {
        ItemTemplates<GenericRecord, JsonNode> templates = getGenericRecordJsonNodeTemplates(template);
        RecordMapper<GenericRecord, JsonNode> mapper = RecordMapper
                .<GenericRecord, JsonNode>builder()
                .withSelectors(templates.selectors())
                .build();

        ConsumerRecord<GenericRecord, JsonNode> incomingRecord = record("topic",
                GenericRecordProvider.RECORD, JsonNodeProvider.RECORD);
        MappedRecord mapped = mapper.map(incomingRecord);
        Item subscribedItem = Items.itemFrom(subscribingItem, new Object());

        assertThat(templates.matches(subscribedItem)).isEqualTo(canSubscribe);

        Stream<Item> expandedItem = templates.expand(mapped);
        Optional<Item> first = expandedItem.findFirst();

        assertThat(first.isPresent()).isTrue();

        assertThat(first.get().matches(subscribedItem)).isEqualTo(exandable);
    }
}
