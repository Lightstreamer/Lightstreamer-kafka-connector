package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

@Tag("unit")
public class ItemTest {

    static List<Value> mkList(String... suffix) {
        return Arrays.stream(suffix).map(ItemTest::value).toList();
    }

    static String name(String suffix) {
        return suffix.transform(s -> "name".concat(suffix));
    }

    static String text(String suffix) {
        return suffix.transform(s -> "text".concat(suffix));
    }

    static Value value(String s) {
        return Value.of(name(s), text(s));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT      | EXPECTED_PREFIX
            item       | item
            item-first | item-first
            item_123_  | item_123_
            item-      | item-
            prefix-<>  | prefix
            """)
    public void shouldMakeItemWitNoValues(String input, String expectedPrefix) {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);
        assertThat(item.core().prefix()).isEqualTo(expectedPrefix);
        assertThat(item.core().keys()).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                | EXPECTED_NAME |   EXPECTED_VALUE
            item-<name=field1>   | name          |   field1
            item-<height=12.34>  | height        |   12.34
            item-<test=\\>       | test          |   \\
            item-<test="">       | test          |   ""
            item-<test=>>        | test          |   >
            item-<test=value,>   | test          |   value
            """)
    public void shouldMakeItemWithSelector(String input, String expectedName, String expectedValue) {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);

        List<Value> values = item.values();
        assertThat(values).containsExactly(Value.of(expectedName, expectedValue));
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                             | EXPECTED_NAME1 |   EXPECTED_VALUE1 |  EXPECTED_NAME2 |  EXPECTED_VALUE2
            item-<name1=field1,name2=field2>  | name1          |   field1          |  name2       |  field2
            """)
    public void shouldMakeItemWithMoreSelectors(String input, String par1, String val1, String par2, String value) {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);

        List<Value> values = item.values();
        assertThat(values).containsExactly(Value.of(par1, val1), Value.of(par2, value));
    }

    @Test
    public void shouldHaveCore() {
        Item item = new Item("source", "item", mkList("A", "B"));
        BasicItem core = item.core();
        assertThat(core).isNotNull();
        assertThat(core.keys()).containsExactly(name("B"), name("A"));
    }

    @Test
    public void shouldHaveValues() {
        Item item = new Item("source", "item", mkList("A", "B"));
        assertThat(item.values()).isNotEmpty();
        assertThat(item.values()).containsExactly(value("A"), value("B"));
    }

    @Test
    public void shouldMatchOneKey() {
        Item item1 = new Item("source", "item", mkList("1"));
        Item item2 = new Item("source", "item", mkList("1"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).hasSize(1);
        assertThat(matchResult.matchedKeys()).contains(name("1"));
    }

    @Test
    public void shouldMatchMoreKeys() {
        Item item1 = new Item("source", "prefix", mkList("1", "2"));
        Item item2 = new Item("source", "prefix", mkList("1", "2"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).containsExactly(name("1"), name("2"));
    }

    @Test
    public void shouldMatchSmallerKeySet() {
        Item item1 = new Item("source", "item", mkList("1", "2", "3"));
        Item item2 = new Item("source", "item", mkList("1", "2"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isTrue();
        assertThat(matchResult.matchedKeys()).containsExactly(name("1"), name("2"));
    }

    @Test
    public void shouldNotMatchKey() {
        Item item1 = new Item("source", "prefix", mkList("1"));
        Item item2 = new Item("source", "prefix", mkList("2"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldNotMatchPrefix() {
        Item item1 = new Item("source", "prefix1", mkList("1"));
        Item item2 = new Item("source", "prefix2", mkList("2"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldPartiallyMatchKeys() {
        Item item1 = new Item("source", "prefix", mkList("1", "2"));
        Item item2 = new Item("source", "prefix", mkList("2", "3"));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).containsExactly(name("2"));
    }
}
