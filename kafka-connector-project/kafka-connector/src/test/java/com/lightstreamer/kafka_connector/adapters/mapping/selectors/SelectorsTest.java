
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka_connector.adapters.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class SelectorsTest {

    static ConnectorConfig avroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc"));
    }

    static Stream<Arguments> stringSelectorsArguments() {
        return Stream.of(
                arguments(Collections.emptyMap(), Schema.empty("schema"), Collections.emptySet()),
                arguments(
                        Map.of("name", "VALUE"),
                        Schema.from("schema", Set.of("name")),
                        Set.of(new SimpleValue("name", "aValue"))),
                arguments(
                        Map.of("value", "VALUE", "key", "KEY"),
                        Schema.from("schema", Set.of("value", "key")),
                        Set.of(new SimpleValue("key", "aKey"), new SimpleValue("value", "aValue"))),
                arguments(
                        Map.of(
                                "timestamp",
                                "TIMESTAMP",
                                "partition",
                                "PARTITION",
                                "topic",
                                "TOPIC"),
                        Schema.from("schema", Set.of("timestamp", "partition", "topic")),
                        Set.of(
                                new SimpleValue("partition", "150"),
                                new SimpleValue("topic", "record-topic"),
                                new SimpleValue("timestamp", "-1"))));
    }

    @ParameterizedTest
    @MethodSource("stringSelectorsArguments")
    public void shouldCreateAndExtractValues(
            Map<String, String> expressions, Schema expectedSchema, Set<Value> expectedValues) {
        Selectors<String, String> selectors =
                Selectors.from(SelectorsSuppliers.string(), "schema", expressions);
        assertThat(selectors.schema()).isEqualTo(expectedSchema);

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        ValuesContainer values = selectors.extractValues(kafkaRecord);

        assertThat(values.selectors()).isSameInstanceAs(selectors);
        Set<Value> values2 = values.values();
        assertThat(values2).isEqualTo(expectedValues);
    }

    static Stream<Arguments> wrongArguments() {
        return Stream.of(
                arguments(
                        Map.of("name", "VALUE."),
                        "Found the invalid expression [VALUE.] while evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE.."),
                        "Found the invalid expression [VALUE..] with missing tokens while"
                                + " evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE"),
                        "Found the invalid expression [VALUE] while evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE.attrib[]"),
                        "Found the invalid indexed expression [VALUE.attrib[]] while evaluating"
                                + " [name]"),
                arguments(
                        Map.of("name", "VALUE.attrib[0]xsd"),
                        "Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating"
                                + " [name]"),
                arguments(
                        Map.of("name", "VALUE.attrib[1]xsd"),
                        "Found the invalid indexed expression [VALUE.attrib[1]xsd] while evaluating"
                                + " [name]"),
                // arguments(
                //         Map.of("name", "VALUE.attrib[1]."),
                //         "Found the invalid indexed expression [VALUE.attrib[1]xsd] while
                // evaluating"
                //                 + " [name]"),
                // arguments(
                //         Map.of("name", "VALUE.attrib.-"),
                //         "Found the invalid indexed expression [VALUE.attrib[1]xsd] while
                // evaluating"
                //                 + " [name]"),
                arguments(
                        Map.of("name", "KEY."),
                        "Found the invalid expression [KEY.] while evaluating [name]"),
                arguments(
                        Map.of("name", "KEY.."),
                        "Found the invalid expression [KEY..] with missing tokens while evaluating"
                                + " [name]"),
                arguments(
                        Map.of("name", "KEY"),
                        "Found the invalid expression [KEY] while evaluating [name]"),
                arguments(
                        Map.of("name", "wrong"),
                        "Found the invalid expression [wrong] while evaluating [name]"),
                arguments(
                        Map.of("name", "\"\""),
                        "Found the invalid expression [\"\"] while evaluating [name]"));
    }

    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateGenericRecordSelectors(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception =
                assertThrows(
                        ExpressionException.class,
                        () ->
                                Selectors.from(
                                        SelectorsSuppliers.avro(avroConfig()), "schema", input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateJsonNodeSelectors(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception =
                assertThrows(
                        ExpressionException.class,
                        () ->
                                Selectors.from(
                                        SelectorsSuppliers.json(ConnectorConfigProvider.minimal()),
                                        "schema",
                                        input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> wrongArgumentsProviderForStringSelectors() {
        return Stream.of(
                arguments(
                        Map.of("name", "VALUE."),
                        "Found the invalid expression [VALUE.] while evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE.."),
                        "Found the invalid expression [VALUE..] while evaluating [name]"),
                arguments(
                        Map.of("name", "KEY."),
                        "Found the invalid expression [KEY.] while evaluating [name]"),
                arguments(
                        Map.of("name", "KEY.."),
                        "Found the invalid expression [KEY..] while evaluating [name]"),
                arguments(
                        Map.of("name", "wrong"),
                        "Found the invalid expression [wrong] while evaluating [name]"));
    }

    @ParameterizedTest
    @MethodSource("wrongArgumentsProviderForStringSelectors")
    public void shouldNotCreateStringSelectors(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException exception =
                assertThrows(
                        ExpressionException.class,
                        () -> Selectors.from(SelectorsSuppliers.string(), "schema", input));
        assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
