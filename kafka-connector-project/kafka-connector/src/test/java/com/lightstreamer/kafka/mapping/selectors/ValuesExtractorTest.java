
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

package com.lightstreamer.kafka.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class ValuesExtractorTest {

    static final String TEST_SCHEMA = "schema";

    static ConnectorConfig avroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc"));
    }

    static <K, V> ValuesExtractor<K, V> extractor(
            Map<String, String> expressions, SelectorSuppliers<K, V> suppliers) {
        return ValuesExtractor.<K, V>builder()
                .withSuppliers(suppliers)
                .withSchemaName(TEST_SCHEMA)
                .withExpressions(expressions)
                .build();
    }

    static Stream<Arguments> stringExtractorArguments() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(), Schema.empty(TEST_SCHEMA), Collections.emptySet()),
                arguments(
                        Map.of("name", "VALUE"),
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Set.of(Value.of("name", "aValue"))),
                arguments(
                        Map.of("value", "VALUE", "key", "KEY"),
                        Schema.from(TEST_SCHEMA, Set.of("value", "key")),
                        Set.of(Value.of("key", "aKey"), Value.of("value", "aValue"))),
                arguments(
                        Map.of("value1", "VALUE", "key1", "KEY"),
                        Schema.from(TEST_SCHEMA, Set.of("value1", "key1")),
                        Set.of(Value.of("key1", "aKey"), Value.of("value1", "aValue"))),
                arguments(
                        Map.of(
                                "timestamp",
                                "TIMESTAMP",
                                "partition",
                                "PARTITION",
                                "topic",
                                "TOPIC"),
                        Schema.from(TEST_SCHEMA, Set.of("timestamp", "partition", "topic")),
                        Set.of(
                                Value.of("partition", "150"),
                                Value.of("topic", "record-topic"),
                                Value.of("timestamp", "-1"))));
    }

    @ParameterizedTest
    @MethodSource("stringExtractorArguments")
    public void shouldCreateAndExtractValues(
            Map<String, String> expressions, Schema expectedSchema, Set<Value> expectedValues) {
        ValuesExtractor<String, String> extractor =
                extractor(expressions, TestSelectorSuppliers.string());

        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        ValuesContainer values = extractor.extractValues(kafkaRecord);

        assertThat(values.extractor()).isSameInstanceAs(extractor);
        Set<Value> values2 = values.values();
        assertThat(values2).isEqualTo(expectedValues);
    }

    static Stream<Arguments> wrongStringExtractorArguments() {
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
    @MethodSource("wrongStringExtractorArguments")
    public void shouldNotCreateStringExtractor(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class,
                        () -> extractor(input, TestSelectorSuppliers.string()));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> wrongArguments() {
        return Stream.of(
                arguments(
                        Map.of("name", "VALUE."),
                        "Found the invalid expression [VALUE.] while evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE.."),
                        "Found the invalid expression [VALUE..] with missing tokens while evaluating [name]"),
                arguments(
                        Map.of("name", "VALUE"),
                        "Found the invalid expression [VALUE] while evaluating [name]"),
                // arguments(
                //         Map.of("name", "VALUE.attrib[]"),
                //         "Found the invalid indexed expression [VALUE.attrib[]] while evaluating"
                //                 + " [name]"),
                // arguments(
                //         Map.of("name", "VALUE.attrib[0]xsd"),
                //         "Found the invalid indexed expression [VALUE.attrib[0]xsd] while
                // evaluating"
                //                 + " [name]"),
                // arguments(
                //         Map.of("name", "VALUE.attrib[1]xsd"),
                //         "Found the invalid indexed expression [VALUE.attrib[1]xsd] while
                // evaluating"
                //                 + " [name]"),
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
                        "Found the invalid expression [KEY..] with missing tokens while evaluating [name]"),
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
    public void shouldNotCreateGenericRecordExtractor(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class,
                        () -> extractor(input, TestSelectorSuppliers.avro(avroConfig())));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest
    @MethodSource("wrongArguments")
    public void shouldNotCreateJsonNodeExtractor(
            Map<String, String> input, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class,
                        () ->
                                extractor(
                                        input,
                                        TestSelectorSuppliers.json(
                                                ConnectorConfigProvider.minimal())));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
