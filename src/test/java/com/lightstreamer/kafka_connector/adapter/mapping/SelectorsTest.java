package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;

public class SelectorsTest {

    static Stream<Arguments> providerForStringSelectors() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Schema.empty()),
                arguments(
                        Map.of("name", "VALUE"),
                        Schema.of("name")),
                arguments(
                        Map.of("value", "VALUE",
                                "key", "KEY"),
                        Schema.of("value", "key")),
                arguments(
                        Map.of("timestamp", "TIMESTAMP",
                                "partition", "PARTITION",
                                "topic", "TOPIC"),
                        Schema.of("timestamp", "partition", "topic")));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("providerForStringSelectors")
    public void shouldCreate(Map<String, String> input, Schema expected) {
        Selectors<String, String> selectors = Selectors.from(SelectorsSupplier.string(), input);
        assertThat(selectors.schema()).isEqualTo(expected);
    }

    static Stream<Arguments> wrongArguments() {
        return Stream.of(
                arguments(Map.of("name", "VALUE."), "Incomplete expression"),
                arguments(Map.of("name", "VALUE.."), "Tokens cannot be blank"),
                arguments(Map.of("name", "VALUE"), "Invalid expression"),
                arguments(Map.of("name", "KEY."), "Incomplete expression"),
                arguments(Map.of("name", "KEY.."), "Tokens cannot be blank"),
                arguments(Map.of("name", "KEY"), "Invalid expression"),
                arguments(Map.of("name", "wrong"), "Invalid expression"));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateGenericRecordSelectors(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSupplier.genericRecord(), input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateJsonModeSelectors(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSupplier.jsonNode(), input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> wrongArgumentsProviderForStringSelectors() {
        return Stream.of(
                arguments(Map.of("name", "VALUE."), "Invalid expression"),
                arguments(Map.of("name", "VALUE.."), "Invalid expression"),
                arguments(Map.of("name", "KEY."), "Invalid expression"),
                arguments(Map.of("name", "KEY.."), "Invalid expression"),
                arguments(Map.of("name", "wrong"), "Invalid expression"));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArgumentsProviderForStringSelectors")
    public void shouldNotCreate3(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSupplier.string(), input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

}
