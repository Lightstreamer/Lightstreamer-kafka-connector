package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.truth.BooleanSubject;
import com.lightstreamer.kafka_connector.adapter.consumers.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordInspector.Builder;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.avro.GenericRecordKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.avro.GenericRecordValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecordProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;

public class ItemTemplateTest {

    static ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>("topic", 0, 0, key, value);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            INPUT,      EXPECTED_PREFIX
            item,       item
            item-first, item-first
            item_123_,  item_123_
            item-,      item-
            prefix-#{}, prefix
            """)
    public void shouldMakeItemWitNoSelectors(String template, String expectedPrefix) {
        Builder<String, String> builder = RecordInspector.stringSelectorsBuilder();
        TopicMapping tm = new TopicMapping("topic", List.of(template));
        List<ItemTemplate<String, String>> items = ItemTemplate.fromTopicMappings(List.of(tm), builder);
        assertThat(items.get(0)).isNotNull();
        assertThat(items.get(0).topic()).isEqualTo("topic");
        assertThat(items.get(0).prefix()).isEqualTo(expectedPrefix);
        assertThat(items.get(0).schemas()).isEmpty();
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = { "a,", ".", "|", "@" })
    public void shouldNotMakeItemDueToInvalidTemplate(String invalidTemplate) {
        TopicMapping tm = new TopicMapping("topic", List.of(invalidTemplate));
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> ItemTemplate.fromTopicMappings(List.of(tm),
                        RecordInspector.noSelectorsBuilder()));
        assertThat(exception.getMessage()).isEqualTo("Invalid item");
    }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME | EXPECTED_SELECTOR
    // item-#{name=VALUE.field1} | name | VALUE.field1
    // item-#{name=VALUE.field[0]} | name | VALUE.field[0]
    // item-#{alias=VALUE.count} | alias | VALUE.count
    // item-#{alias=VALUE.count.test[*]} | alias | VALUE.count.test[*]
    // """)
    // public void shouldMakeItemWithValueSelector(String template, String
    // expectedName, String expectedSelector) {
    // List<ItemTemplate<String, String>> item = ItemTemplate.fromTopicMappings(
    // List.of(new TopicMapping("topic", List.of(template))),
    // RecordInspector.stringSelectorsBuilder());
    // assertThat(item).isNotNull();

    // List<ValueSelector<String>> valueSelectors =
    // item.inspector().valueSelectors();
    // assertThat(valueSelectors).hasSize(1);

    // ValueSelector<String> valueSelector = valueSelectors.get(0);
    // assertThat(valueSelector.name()).isEqualTo(expectedName);
    // assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
    // }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME | EXPECTED_SELECTOR
    // item-#{key=KEY} | key | KEY
    // """)
    // public void shouldMakeItemWithKeySelector(String template, String
    // expectedName, String expectedSelector) {
    // ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template,
    // RecordInspector.stringSelectorsBuilder());
    // assertThat(item).isNotNull();

    // List<KeySelector<String>> valueSelectors = item.inspector().keySelectors();
    // assertThat(valueSelectors).hasSize(1);

    // KeySelector<String> valueSelector = valueSelectors.get(0);
    // assertThat(valueSelector.name()).isEqualTo(expectedName);
    // assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
    // }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME1 | EXPECTED_SELECTOR1 | EXPECTED_NAME2 |
    // EXPECTED_SELECTOR2
    // item-#{name1=VALUE.field1,name2=VALUE.field2} | name1 | VALUE.field1 | name2
    // | VALUE.field2
    // item-#{name1=VALUE.field1[0],name2=VALUE.field2.otherField} | name1 |
    // VALUE.field1[0] | name2 | VALUE.field2.otherField
    // """)
    // public void shouldMakeItemWithMoreValueSelectors(String template, String
    // expectedName1,
    // String expectedSelector1,
    // String expectedName2, String expectedSelector2) {
    // ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template,
    // builder());
    // assertThat(item).isNotNull();

    // List<? extends Selector<String>> keySelectors =
    // item.inspector().valueSelectors();
    // assertThat(keySelectors).hasSize(2);

    // Selector<String> valueSelector1 = keySelectors.get(0);
    // assertThat(valueSelector1.name()).isEqualTo(expectedName1);
    // assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

    // Selector<String> valueSelector2 = keySelectors.get(1);
    // assertThat(valueSelector2.name()).isEqualTo(expectedName2);
    // assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
    // }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME1 | EXPECTED_SELECTOR1 | EXPECTED_NAME2 |
    // EXPECTED_SELECTOR2
    // item-#{name1=TIMESTAMP,name2=PARTITION} | name1 | TIMESTAMP | name2 |
    // PARTITION
    // item-#{name1=TOPIC,name2=PARTITION} | name1 | TOPIC | name2 | PARTITION
    // """)
    // public void shouldMakeItemWithMoreInfoSelectors(String template, String
    // expectedName1, String expectedSelector1,
    // String expectedName2, String expectedSelector2) {
    // ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template,
    // builder());
    // assertThat(item).isNotNull();

    // List<? extends Selector<ConsumerRecord<?, ?>>> infoSelectors =
    // item.inspector().metaSelectors();
    // assertThat(infoSelectors).hasSize(2);

    // Selector<ConsumerRecord<?, ?>> valueSelector1 = infoSelectors.get(0);
    // assertThat(valueSelector1.name()).isEqualTo(expectedName1);
    // assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

    // Selector<ConsumerRecord<?, ?>> valueSelector2 = infoSelectors.get(1);
    // assertThat(valueSelector2.name()).isEqualTo(expectedName2);
    // assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
    // }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME1 | EXPECTED_SELECTOR1 | EXPECTED_NAME2 |
    // EXPECTED_SELECTOR2
    // item-#{name1=KEY.field1,name2=KEY.field2} | name1 | KEY.field1 | name2 |
    // KEY.field2
    // item-#{name1=KEY.field1[0],name2=KEY.field2.otherField} | name1 |
    // KEY.field1[0] | name2 | KEY.field2.otherField
    // """)
    // public void shouldMakeItemWithMoreKeySelectors(String template, String
    // expectedName1, String expectedSelector1,
    // String expectedName2, String expectedSelector2) {
    // ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template,
    // builder());
    // assertThat(item).isNotNull();

    // List<? extends Selector<String>> keySelectors =
    // item.inspector().keySelectors();
    // assertThat(keySelectors).hasSize(2);

    // Selector<String> valueSelector1 = keySelectors.get(0);
    // assertThat(valueSelector1.name()).isEqualTo(expectedName1);
    // assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

    // Selector<String> valueSelector2 = keySelectors.get(1);
    // assertThat(valueSelector2.name()).isEqualTo(expectedName2);
    // assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
    // }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT               | EXPECTED_NAME | EXPECTED_SELECTOR
            item-#{name1=VALUE} | name1          | VALUE
            """)
    public void shouldExpandOneValue(String template, String expectedName, String expectedSelector) {
        ItemTemplate<String, String> itemTemplate = ItemTemplate.create("topic",
                template, RecordInspector.stringSelectorsBuilder());
        Item expanded = itemTemplate.expand(record(null, "record-value"));
        assertThat(expanded.values())
                .containsExactly(
                        new SimpleValue(expectedName, "record-value"));
    }

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
    // INPUT | EXPECTED_NAME | EXPECTED_SELECTOR
    // item-#{name1=VALUE.field1,name2=VALUE.field2} | name | VALUE.field1
    // """)
    // public void shouldExpandMoreValues(String template) {
    // ItemTemplate<String, String> itemTemplate = ItemTemplate.create("topic",
    // template, RecordInspector.stringSelectorsBuilder());
    // Item expanded = itemTemplate.expand(record("key", "record-value"));
    // assertThat(expanded.values())
    // .containsExactly(
    // new SimpleValue("name1", "extracted <record-value>"),
    // new SimpleValue("name2", "extracted <record-value>"));
    // }

    // @Test
    // public void shouldNotExpand() {
    // ItemTemplate<String, String> itemTemplate = ItemTemplate.makeNew("topic",
    // "template", builder());
    // Item expanded = itemTemplate.expand(record("key", "record-value"));
    // assertThat(expanded.values()).isEmpty();
    // }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
        TEMPLATE                                                                         | SUBCRIBING_ITEM                        | MATCH
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<name=joe>                  | true
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<name=joe,child=alex>       | true
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<child=alex>                | true
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro                             | true
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-                            | false
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<keyName=joe>               | true
            kafka-avro-#{keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<keyName=joe,child=alex>    | true
            kafka-avro-#{keyName=KEY.name,child=VALUE.children[1].children[0].name}      | kafka-avro-<keyName=joe,child=gloria>  | true
            kafka-avro-#{keyName=KEY.name,child=VALUE.children[1].children[1].name}      | kafka-avro-<keyName=joe,child=terence> | true
            kafka-avro-#{keyName=KEY.name,child=VALUE.children[1].children[1].name}      | kafka-avro-<keyName=joe,child=carol>   | false
            kafka-avro-#{child=VALUE.children[1].children[1].name}                       | kafka-avro-<keyName=joe,child=terence> | false
            kafka-avro-#{child=VALUE.children[1].children[1].name}                       | kafka-avro-<child=terence>             | true
            kafka-avro-#{child=VALUE.children[1].children[2].name}                       | kafka-avro-<child=terence>             | true
            """)
    public void shouldExpand(String template, String subscribingItem, boolean matched) {
        RecordInspector.Builder<GenericRecord, GenericRecord> builder = RecordInspector.builder(
                new GenericRecordKeySelectorSupplier(),
                new GenericRecordValueSelectorSupplier());

        ItemTemplate<GenericRecord, GenericRecord> itemTemplate = ItemTemplate.create("topic", template, builder);

        ConsumerRecord<GenericRecord, GenericRecord> inputRecord = ConsumerRecordProvider
                .recordWithGenericRecordPair(GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        Item subscribedItem = Item.of(subscribingItem, new Object());
        Item expandedItem = itemTemplate.expand(inputRecord);
        MatchResult match = expandedItem.match(subscribedItem);
        BooleanSubject assertion = assertThat(match.matched());
        if (matched) {
            assertion.isTrue();
        } else {
            assertion.isFalse();
        }
    }
}
