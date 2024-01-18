package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapter.test_utils.SelectorsSuppliers;

public class SelectorsTest {

    static ConnectorConfig avroConfig() {
        return ConnectorConfigProvider.minimalWith(
                Map.of(ConnectorConfig.ADAPTER_DIR, "src/test/resources",
                        ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc",
                        ConnectorConfig.VALUE_SCHEMA_FILE, "value.avsc"));
    }

    static Stream<Arguments> stringSelectorsArguments() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Schema.empty("schema"),
                        Collections.emptySet()),
                arguments(
                        Map.of("name", "VALUE"),
                        Schema.from("schema", Set.of("name")),
                        Set.of(new SimpleValue("name", "aValue"))),
                arguments(
                        Map.of("value", "VALUE",
                                "key", "KEY"),
                        Schema.from("schema", Set.of("value", "key")),
                        Set.of(new SimpleValue("key", "aKey"),
                                new SimpleValue("value", "aValue"))),
                arguments(
                        Map.of("timestamp", "TIMESTAMP",
                                "partition", "PARTITION",
                                "topic", "TOPIC"),
                        Schema.from("schema", Set.of("timestamp", "partition", "topic")),
                        Set.of(new SimpleValue("partition", "150"),
                                new SimpleValue("topic", "record-topic"),
                                new SimpleValue("timestamp", "-1"))));

    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("stringSelectorsArguments")
    public void shouldCreateAndExtractValues(Map<String, String> expressions, Schema expectedSchema,
            Set<Value> expectedValues) {
        Selectors<String, String> selectors = Selectors.from(
                SelectorsSuppliers.string(), "schema", expressions);
        assertThat(selectors.schema()).isEqualTo(expectedSchema);

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        ValuesContainer values = selectors.extractValues(kafkaRecord);

        assertThat(values.selectors()).isSameInstanceAs(selectors);
        Set<Value> values2 = values.values();
        assertThat(values2).isEqualTo(expectedValues);
    }

    static Stream<Arguments> wrongArguments() {
        return Stream.of(
                arguments(Map.of("name", "VALUE."), "Incomplete expression"),
                arguments(Map.of("name", "VALUE.."), "Tokens cannot be blank"),
                arguments(Map.of("name", "VALUE"), "Found the invalid expression [VALUE] while evaluating [name]"),
                arguments(Map.of("name", "KEY."), "Incomplete expression"),
                arguments(Map.of("name", "KEY.."), "Tokens cannot be blank"),
                arguments(Map.of("name", "KEY"), "Found the invalid expression [KEY] while evaluating [name]"),
                arguments(Map.of("name", "wrong"), "Found the invalid expression [wrong] while evaluating [name]")
        );
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateGenericRecordSelectors(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSuppliers.avro(avroConfig()),
                        "schema", input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateJsonNodeSelectors(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSuppliers.json(ConnectorConfigProvider.minimal()), "schema",
                        input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> wrongArgumentsProviderForStringSelectors() {
        return Stream.of(
                arguments(Map.of("name", "VALUE."), "Found the invalid expression [VALUE.] while evaluating [name]"),
                arguments(Map.of("name", "VALUE.."), "Found the invalid expression [VALUE..] while evaluating [name]"),
                arguments(Map.of("name", "KEY."), "Found the invalid expression [KEY.] while evaluating [name]"),
                arguments(Map.of("name", "KEY.."), "Found the invalid expression [KEY..] while evaluating [name]"),
                arguments(Map.of("name", "wrong"), "Found the invalid expression [wrong] while evaluating [name]"));
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("wrongArgumentsProviderForStringSelectors")
    public void shouldNotCreateStringSelectors(Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception = assertThrows(ExpressionException.class,
                () -> Selectors.from(SelectorsSuppliers.string(), "schema", input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

}
