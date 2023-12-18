package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.EvaluationException;

public class ItemTest {

    @Tag("unit")
    @Test
    public void shouldHaveSchemaAndValues() {
        Item item = new Item("source", "item", Map.of("a", "A", "b", "B"));
        Schema schema = item.schema();
        assertThat(schema).isNotNull();
        assertThat(schema.prefix()).isEqualTo("item");
        assertThat(schema.keys()).isNotEmpty();
        assertThat(schema.keys()).containsExactly("a", "b");
        assertThat(item.values()).isNotEmpty();
        assertThat(item.values()).containsExactly("b", "B", "a", "A");
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldMatch(Map<String, String> values1, Map<String, String> values2, List<String> xpectedKey) {
        Item item1 = new Item("source", "item", values1);
        Item item2 = new Item("source", "item", values2);
        assertThat(item1.matches(item2)).isTrue();
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        Map.of("n1", "1"),
                        Map.of("n1", "1"),
                        List.of("n1")),
                arguments(
                        Map.of("n1", "1", "n2", "2"),
                        Map.of("n1", "1", "n2", "2"),
                        List.of("n1", "n2")),
                arguments(
                        Map.of("n1", "1", "n2", "2", "n3", "3"),
                        Map.of("n1", "1", "n2", "2"),
                        List.of("n1", "n2")));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("notMatchingProvider")
    public void shouldNotMatch(Map<String, String> values1, Map<String, String> values2) {
        Item item1 = new Item("source", "prefix", values1);
        Item item2 = new Item("source", "prefix", values2);
        assertThat(item1.matches(item2)).isFalse();
    }

    static Stream<Arguments> notMatchingProvider() {
        return Stream.of(
                arguments(
                        Map.of("n1", "1"),
                        Map.of("n2", "2")),
                arguments(
                        Map.of("key", "value1"),
                        Map.of("key", "value2")));
    }

    @Tag("unit")
    @Test
    public void shouldNotMatcDueToDifferentPrefix() {
        Map<String, String> sameValues = Map.of("n1", "1");
        Item item1 = new Item("source", "aPrefix", sameValues);
        Item item2 = new Item("source", "anotherPrefix", sameValues);
        assertThat(item1.matches(item2)).isFalse();
    }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT      | EXPECTED_PREFIX
            item       | item
            item-first | item-first
            item_123_  | item_123_
            item-      | item-
            prefix-<>  | prefix
            """)
    public void shouldMakeWithEmptySchemaKeys(String input, String expectedPrefix)
            throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().prefix()).isEqualTo(expectedPrefix);
        assertThat(item.schema().keys()).isEmpty();
        assertThat(item.values()).isEmpty();
    }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                       | EXPECTED_PREFIX | EXPECTED_NAME | EXPECTED_VALUE
            item-<name=field1>          | item            | name          | field1
            item-first-<height=12.34>   | item-first      | height        | 12.34
            item_123_-<test=\\>         | item_123_       | test          | \\
            item-<test="">              | item            | test          | ""
            prefix-<test=>>             | prefix          | test          | >
            item-<test=value,>          | item            | test          | value
            item-                       | item-           |               |
            item-<>                     | item            |               |
            """)
    public void shouldMakeWithValue(String input, String expectedPrefix, String expectedName,
            String expectedValue)
            throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().prefix()).isEqualTo(expectedPrefix);
        if (expectedName != null && expectedValue != null) {
            assertThat(item.values()).containsExactly(expectedName, expectedValue);
        } else {
            assertThat(item.schema().keys()).isEmpty();
        }
    }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                            | EXPECTED_NAME1 | EXPECTED_VALUE1 | EXPECTED_NAME2 | EXPECTED_VALUE2
            item-<name1=field1,name2=field2> | name1          | field1          | name2          | field2
            """)
    public void shouldMakeWithMoreValues(String input, String name1, String val1, String name2, String value2)
            throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);

        assertThat(item.values()).containsExactly(name1, val1, name2, value2);
    }

    @Tag("unit")
    @Test
    public void shouldNotCreateDueToDuplicatedKeys() {
        assertThrows(EvaluationException.class, () -> {
            Item.of("item-<name1=field1,name1=field2>", new Object());
        });
    }
}
