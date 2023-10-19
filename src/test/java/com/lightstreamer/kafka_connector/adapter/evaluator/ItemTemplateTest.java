package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.consumers.GenericRecordSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.test_utils.IdentityValueSelector;

public class ItemTemplateTest {

    static List<? extends ValueSelector<String, String>> mkList(String... keys) {
        return Arrays.stream(keys).map(key -> new IdentityValueSelector(key, "expr")).toList();
    }

    static ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>("topic", 0, 0, key, value);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            INPUT,      EXPECTED
            item,       item
            item-first, item-first
            item_123_,  item_123_
            item-,      item-
            prefix-${}, prefix
            """)
    public void shouldMakeItemWitNoSelectors(String template, String expectedPrefix) {
        ItemTemplate<String> item = ItemTemplate.makeNew("topic", template, IdentityValueSelector::new);
        assertThat(item).isNotNull();
        assertThat(item.topic()).isEqualTo("topic");
        assertThat(item.prefix()).isEqualTo(expectedPrefix);
        assertThat(item.schemas()).isEmpty();
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    @ValueSource(strings = { "a,", ".", "|", "@" })
    public void shouldNotMakeItemDueToInvalidTemplate(String invalidTemplate) {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> ItemTemplate.makeNew("topic", invalidTemplate, IdentityValueSelector::new));
        assertThat(exception.getMessage()).isEqualTo("Invalid template");
    }

    @ParameterizedTest
    @NullSource
    public void shouldNotMakeItemDueToInvalidTopic(String topic) {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> ItemTemplate.makeNew(topic, "template", IdentityValueSelector::new));
        assertThat(exception.getMessage()).isEqualTo("Invalid topic");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                       | EXPECTED_NAME |   EXPECTED_SELECTOR
            item-${name=Value.field1}   | name          |   Value.field1
            item-${name=Value.field[0]} | name          |   Value.field[0]
            item-${alias=count}         | alias         |   count
            item-${alias=count.test[*]} | alias         |   count.test[*]
            """)
    public void shouldMakeItemWithSelector(String template, String expectedName, String expectedSelector) {
        ItemTemplate<String> item = ItemTemplate.makeNew("topic", template, IdentityValueSelector::new);
        assertThat(item).isNotNull();

        List<? extends ValueSelector<String, String>> valueSelectors = item.valueSelectors();
        assertThat(valueSelectors).hasSize(1);

        ValueSelector<String, String> valueSelector = valueSelectors.get(0);
        assertThat(valueSelector.name()).isEqualTo(expectedName);
        assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                                                       | EXPECTED_NAME1 |   EXPECTED_SELECTOR1 |  EXPECTED_NAME2 |  EXPECTED_SELECTOR2
            item-${name1=Value.field1,name2=Value.field2}               | name1          |   Value.field1       |  name2          |  Value.field2
            item-${name1=Value.field1[0],name2=Value.field2.otherField} | name1          |   Value.field1[0]    |  name2          |  Value.field2.otherField
            """)
    public void shouldMakeItemWithMoreSelectors(String template, String expectedName1, String expectedSelector1,
            String expectedName2, String expectedSelector2) {
        ItemTemplate<String> item = ItemTemplate.makeNew("topic", template, IdentityValueSelector::new);
        assertThat(item).isNotNull();

        List<? extends ValueSelector<String, String>> valueSelectors = item.valueSelectors();
        assertThat(valueSelectors).hasSize(2);

        ValueSelector<String, String> valueSelector1 = valueSelectors.get(0);
        assertThat(valueSelector1.name()).isEqualTo(expectedName1);
        assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

        ValueSelector<String, String> valueSelector2 = valueSelectors.get(1);
        assertThat(valueSelector2.name()).isEqualTo(expectedName2);
        assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                       | EXPECTED_NAME |   EXPECTED_SELECTOR
            item-${name1=Value.field1}  | name          |   Value.field1
            """)
    public void shouldExpandOneValue(String template) {
        // ItemTemplate<String> itemTemplate = ItemTemplate.makeNew("topic", "prefix",
        // mkList("name1"), IdentityValueSelector::new);
        ItemTemplate<String> itemTemplate = ItemTemplate.makeNew("topic", template, IdentityValueSelector::new);
        Item expanded = itemTemplate.expand(record("key", "record-value"));
        assertThat(expanded.values())
                .containsExactly(
                        new SimpleValue("name1", "extracted <record-value>"));

    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                                          | EXPECTED_NAME |   EXPECTED_SELECTOR
            item-${name1=Value.field1,name2=Value.field2}  | name          |   Value.field1
            """)
    public void shouldExpandMoreValues(String template) {
        ItemTemplate<String> itemTemplate = ItemTemplate.makeNew("topic", template, IdentityValueSelector::new);
        Item expanded = itemTemplate.expand(record("key", "record-value"));
        assertThat(expanded.values())
                .containsExactly(
                        new SimpleValue("name1", "extracted <record-value>"),
                        new SimpleValue("name2", "extracted <record-value>"));

    }

    @Test
    public void shouldNotExpand() {
        ItemTemplate<String> itemTemplate = ItemTemplate.makeNew("topic", "template", IdentityValueSelector::new);
        Item expanded = itemTemplate.expand(record("key", "record-value"));
        assertThat(expanded.values()).isEmpty();
    }

    @Test
    public void shouldMatchItem() {
        ItemTemplate<GenericRecord> itemTemplate = ItemTemplate
                .makeNew("topci", "kafka-avro-${name=User.name,friend=User.friends}", GenericRecordSelector::new);

        List<? extends ValueSelector<String, GenericRecord>> valueSelectors = itemTemplate.valueSelectors();
        assertThat(valueSelectors).hasSize(2);

        ValueSelector<String, GenericRecord> valueSelector1 = valueSelectors.get(0);
        assertThat(valueSelector1.name()).isEqualTo("name");
        assertThat(valueSelector1.expression()).isEqualTo("User.name");

        ValueSelector<String, GenericRecord> valueSelector2 = valueSelectors.get(1);
        assertThat(valueSelector2.name()).isEqualTo("friend");
        assertThat(valueSelector2.expression()).isEqualTo("User.friends");

        Item item = Item.fromItem("kafka-avro-<name=Gianluca>", new Object());
        MatchResult match = itemTemplate.match(item);
        assertThat(match.matched()).isTrue();

    }
}
