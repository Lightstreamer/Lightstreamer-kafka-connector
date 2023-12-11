package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value.of;
import static org.junit.Assert.assertThrows;
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

import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public class ItemTest {

    static List<Value> values(Value... value) {
        return Arrays.asList(value);
    }

    @Tag("unit")
    @Test
    public void shouldHaveSchemaAndValues() {
        Item item = new Item("source", "item", values(
                of("a", "A"),
                of("b", "B")));
        ItemSchema schema = item.schema();
        assertThat(schema).isNotNull();
        assertThat(schema.prefix()).isEqualTo("item");
        assertThat(schema.keys()).containsExactly("a", "b");
        assertThat(item.values()).isNotEmpty();
        assertThat(item.values()).containsExactly(of("a", "A"), of("b", "B"));
    }

    @Tag("unit")
    @Test
    public void shouldNotCreateDueToDuplicatedKeys() {
        assertThrows(RuntimeException.class, () -> {
            new Item("source", "item", values(
                    of("a", "A"),
                    of("a", "B")));
        });
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldMatchOneKey(List<Value> values1, List<Value> values2, List<String> expectedKey) {
        Item item1 = new Item("source", "item", values1);
        Item item2 = new Item("source", "item", values2);
        assertThat(item1.matches(item2)).isTrue();
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
                        List.of("n1", "n2")));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("notMatchingProvider")
    public void shouldNotMatch(List<Value> values1, List<Value> values2) {
        Item item1 = new Item("source", "prefix", values(of("n1", "1")));
        Item item2 = new Item("source", "prefix", values(of("n2", "2")));
        assertThat(item1.matches(item2)).isFalse();
    }

    static Stream<Arguments> notMatchingProvider() {
        return Stream.of(
                arguments(
                        values(of("n1", "1")),
                        values(of("n2", "2"))),
                arguments(
                        values(of("key", "value1")),
                        values(of("key", "value2"))));
    }

    @Tag("unit")
    @Test
    public void shouldNotMatcDueToDifferentPrefix() {
        List<Value> sameValues = values(of("n1", "1"));
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
    public void shouldMakeItemWitNoValues(String input, String expectedPrefix) throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().prefix()).isEqualTo(expectedPrefix);
        assertThat(item.schema().keys()).isEmpty();
    }

    @Tag("integration")
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
    public void shouldMakeItemWithSelector(String input, String expectedName, String expectedValue) throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);

        List<Value> values = item.values();
        assertThat(values).containsExactly(Value.of(expectedName, expectedValue));
    }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            INPUT                              | EXPECTED_NAME1 |   EXPECTED_VALUE1 |  EXPECTED_NAME2 |  EXPECTED_VALUE2
            item-<name1=field1,name2=field2>   | name1          |   field1          |  name2       |  field2
            """)
    public void shouldMakeItemWithMoreSelectors(String input, String name1, String val1, String name2, String value2) throws EvaluationException {
        Object handle = new Object();
        Item item = Item.of(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.getItemHandle()).isSameInstanceAs(handle);

        List<Value> values = item.values();
        assertThat(values).containsExactly(Value.of(name1, val1), Value.of(name2, value2));
    }
}
