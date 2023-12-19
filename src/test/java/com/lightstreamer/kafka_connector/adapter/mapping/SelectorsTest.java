package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;

public class SelectorsTest {

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Schema.empty()),
                arguments(
                        Map.of("name", "VALUE"),
                        Schema.of("name")),
                arguments(
                        Map.of("value", "VALUE", "key", "KEY"),
                        Schema.of("value", "key")),
                arguments(
                        Map.of("timestamp", "TIMESTAMP", "partition", "PARTITION", "topic", "TOPIC"),
                        Schema.of("timestamp", "partition", "topic"))

        );
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldCreate(Map<String, String> input, Schema expected) {
        Selectors<String, String> selectors = Selectors.from(SelectorsSupplier.stringSelectorsSupplier(), input);
        assertThat(selectors.schema()).isEqualTo(expected);
    }

    static Stream<Map<String, String>> wrongArgumentsProvider() {
        return Stream.of(
                Map.of("name", "VALUE"));
                // Map.of("name", "WRONG"),
                // Map.of("wrong1", "WRONG", "wrong2", "WRONG2"));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArgumentsProvider")
    public void shouldNotCreate(Map<String, String> input) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSupplier.genericRecordSelectorsSupplier(), input));
        // assertThat(exception.getMessage()).isEqualTo("Unparsable expression
        // \"WRONG_EXPRESSION\"");
    }
}
