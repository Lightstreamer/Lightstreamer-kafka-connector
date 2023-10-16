package com.lightstreamer.kafka_connector.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.Item;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemTemplate;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;

class IdentityValueSelector extends BaseValueSelector<String> {

    IdentityValueSelector(String name, String expr) {
        super(name, expr);
    }

    @Override
    public Value extract(String t) {
        return new SimpleValue(name(), t.transform(s -> "extracted <%s>".formatted(s)));
    }

}

public class ItemTemplateTest {

    static List<? extends ValueSelector<String>> mkList(String... keys) {
        return Arrays.stream(keys).map(key -> new IdentityValueSelector(key, "expr")).toList();
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
    public void shouldMakeItemWitEmptySelectors(String template, String expectedPrefix) {
        ItemTemplate<String> item = ItemTemplate.makeNew(template, IdentityValueSelector::new);
        assertThat(item).isNotNull();
        assertThat(item.prefix()).isEqualTo(expectedPrefix);
        assertThat(item.schemas()).isEmpty();
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    @ValueSource(strings = { "a,", ".", "|", "@" })
    public void shouldNotMakeItem(String invalidTemplate) {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> ItemTemplate.makeNew(invalidTemplate, IdentityValueSelector::new));
        assertThat(exception.getMessage()).isEqualTo("Invalid template");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                       | EXPECTED_NAME |   EXPECTED_SELECTOR
            item-${name=Value.field1}   | name          |   Value.field1
            item-${name=Value.field[0]} | name          |   Value.field[0]
            item-${alias=count}         | alias         |   count
            item-${alias=count.test[*]} | alias         |   count.test[*]
            """)
    public void shouldMakeItemWithSelector(@NonNull String template, String expectedName, String expectedSelector) {
        ItemTemplate<String> item = ItemTemplate.makeNew(template, IdentityValueSelector::new);
        assertThat(item).isNotNull();

        List<ValueSelector<String>> valueSelectors = (List<ValueSelector<String>>) item.schemas();
        assertThat(valueSelectors).hasSize(1);

        ValueSelector<String> valueSelector = valueSelectors.get(0);
        assertThat(valueSelector.name()).isEqualTo(expectedName);
        assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true,  delimiter = '|', textBlock = """
            INPUT                                                       | EXPECTED_NAME1 |   EXPECTED_SELECTOR1 |  EXPECTED_NAME2 |  EXPECTED_SELECTOR2
            item-${name1=Value.field1,name2=Value.field2}               | name1          |   Value.field1       |  name2          |  Value.field2
            item-${name1=Value.field1[0],name2=Value.field2.otherField} | name1          |   Value.field1[0]    |  name2          |  Value.field2.otherField
            """)
    public void shouldMakeItemWithMoreSelectors(String template, String expectedName1, String expectedSelector1,
            String expectedName2, String expectedSelector2) {
        ItemTemplate<String> item = ItemTemplate.makeNew(template, IdentityValueSelector::new);
        assertThat(item).isNotNull();

        List<ValueSelector<String>> valueSelectors = (List<ValueSelector<String>>) item.schemas();
        assertThat(valueSelectors).hasSize(2);

        ValueSelector<String> valueSelector1 = valueSelectors.get(0);
        assertThat(valueSelector1.name()).isEqualTo(expectedName1);
        assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

        ValueSelector<String> valueSelector2 = valueSelectors.get(1);
        assertThat(valueSelector2.name()).isEqualTo(expectedName2);
        assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
    }

    @Test
    public void shouldMatchOneKey() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix", mkList("name1"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix", mkList("name1"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).containsExactly("name1");
    }

    @Test
    public void shouldMatchMoreKeys() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix", mkList("name1", "name2"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix", mkList("name1", "name2"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).containsExactly("name1", "name2");
    }

    @Test
    public void shouldMatchSmallerKeySet() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix", mkList("name1", "name2", "name3"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix", mkList("name1", "name2"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).containsExactly("name1", "name2");
    }

    @Test
    public void shouldNotMatchKey() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix", mkList("name1"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix", mkList("name2"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldNotMatchPrefix() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix1", mkList("name1"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix2", mkList("name1"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldPartiallyMatchKeys() {
        ItemTemplate<String> itemTemplate1 = new ItemTemplate<>("prefix", mkList("name1", "name2"));
        ItemTemplate<String> itemTemplate2 = new ItemTemplate<>("prefix", mkList("name2", "name3"));
        MatchResult matchResult = itemTemplate1.match(itemTemplate2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).containsExactly("name2");
    }

    @Test
    public void shouldExpandOneValue() {
        ItemTemplate<String> itemTemplate = new ItemTemplate<>("prefix", mkList("name1"));
        Item expanded = itemTemplate.expand("record-value");
        assertThat(expanded.values())
                .containsExactly(
                        new SimpleValue("name1", "extracted <record-value>"));

    }

    @Test
    public void shouldExpandMoreValues() {
        ItemTemplate<String> itemTemplate = new ItemTemplate<>("prefix", mkList("name1", "name2"));
        Item expanded = itemTemplate.expand("record-value");
        assertThat(expanded.values())
                .containsExactly(
                        new SimpleValue("name1", "extracted <record-value>"),
                        new SimpleValue("name2", "extracted <record-value>"));

    }

    @Test
    public void shouldNotExpand() {
        ItemTemplate<String> itemTemplate = new ItemTemplate<>("prefix", Collections.emptyList());
        Item expanded = itemTemplate.expand("record-value");
        assertThat(expanded.values()).isEmpty();
    }
}
