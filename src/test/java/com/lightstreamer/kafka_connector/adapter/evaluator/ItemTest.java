package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value.of;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

@Tag("unit")
public class ItemTest {

    static List<Value> values(Value... value) {
        return Arrays.asList(value);
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
        Item item = new Item("source", "item", values(
                of("a", "A"),
                of("b", "B")));
        BasicItem core = item.core();
        assertThat(core).isNotNull();
        assertThat(core.keys()).containsExactly("a", "b");
    }

    @Test
    public void shouldHaveValues() {
        Item item = new Item("source", "item", values(
                of("a", "A"),
                of("b", "B")));
        assertThat(item.values()).isNotEmpty();
        assertThat(item.values()).containsExactly(of("a", "A"), of("b", "B"));
    }

    @ParameterizedTest
    @MethodSource("provider")
    public void shouldMatchOneKey(List<Value> values1, List<Value> values2, List<String> expectedKey) {
        Item item1 = new Item("source", "item", values1);
        Item item2 = new Item("source", "item", values2);
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isTrue();
        // assertThat(matchResult.matchedKeys()).hasSize(1);
        assertThat(matchResult.matchedKeys()).containsExactly(expectedKey.toArray(new Object[0]));
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        values(of("n1", "1")),
                        values(of("n1", "1")),
                        List.of("n1")),
                arguments(
                        values(
                                of("n1", "1"),
                                of("n2", "2")),
                        values(
                                of("n1", "1"),
                                of("n2", "2")),
                        List.of("n1", "n2")),
                arguments(
                        values(
                                of("n1", "1"),
                                of("n2", "2"),
                                of("n3", "3")),
                        values(
                                of("n1", "1"),
                                of("n2", "2")),
                        List.of("n1", "n2"))
        );
    }

    @Test
    public void shouldNotMatchAntKey() {
        Item item1 = new Item("source", "prefix", values(of("n1", "1")));
        Item item2 = new Item("source", "prefix", values(of("n2", "2")));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldMatch() {
        Item item1 = new Item("source", "prefix", List.of(Value.of("key", "value1")));
        Item item2 = new Item("source", "prefix", List.of(Value.of("key", "value2")));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldNotMatchPrefix() {
        Item item1 = new Item("source", "prefix1", values(of("n1", "1")));
        Item item2 = new Item("source", "prefix2", values(of("n2", "2")));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).isEmpty();
    }

    @Test
    public void shouldPartiallyMatchKeys() {
        Item item1 = new Item("source", "prefix", values(of("n1", "2"), of("n2", "2")));
        Item item2 = new Item("source", "prefix", values(of("n3", "3"), of("n2", "2")));
        MatchResult matchResult = item1.match(item2);
        assertThat(matchResult.matched()).isFalse();
        assertThat(matchResult.matchedKeys()).containsExactly("n2");
    }
}
