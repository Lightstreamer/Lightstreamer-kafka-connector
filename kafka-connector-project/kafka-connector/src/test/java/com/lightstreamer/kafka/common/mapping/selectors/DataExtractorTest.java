
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;
import static com.lightstreamer.kafka.common.expressions.Expressions.Template;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.EMPTY_MAP;

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DataExtractorTest {

    static final String TEST_SCHEMA = "schema";

    @Test
    public void shouldBuildEqualExtractors() throws ExtractionException {
        DataExtractor<String, String> extractor1 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")));
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")));
        assertThat(extractor1.hashCode()).isEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isTrue();
    }

    static Stream<Arguments> extractorFromExtractionExpressions() {
        return Stream.of(
                arguments(TEST_SCHEMA, EMPTY_MAP, Schema.empty(TEST_SCHEMA), EMPTY_MAP),
                arguments(
                        TEST_SCHEMA,
                        Map.of("name", Expression("VALUE")),
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        "aSchemNAme",
                        Map.of("value", Expression("VALUE"), "key", Expression("KEY")),
                        Schema.from("aSchemNAme", Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        "anotherSchemaName",
                        Map.of("value1", Expression("VALUE"), "key1", Expression("KEY")),
                        Schema.from("anotherSchemaName", Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue")),
                arguments(
                        "mySchemaName",
                        Map.of(
                                "timestamp",
                                Expression("TIMESTAMP"),
                                "partition",
                                Expression("PARTITION"),
                                "topic",
                                Expression("TOPIC")),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")));
    }

    @ParameterizedTest
    @MethodSource("extractorFromExtractionExpressions")
    public void shouldCreateAndExtractValuesExtractionExpressions(
            String schemaName,
            Map<String, ExtractionExpression> expressions,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), schemaName, expressions);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEqualTo(expectedValues);
    }

    static Stream<Arguments> extractorFromSimpleExpression() {
        return Stream.of(
                arguments(
                        TEST_SCHEMA,
                        "name",
                        "VALUE",
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        "aSchemNAme",
                        "value",
                        "VALUE",
                        Schema.from("aSchemNAme", Set.of("value")),
                        Map.of("value", "aValue")),
                arguments(
                        "anotherSchemaName",
                        "value1",
                        "VALUE",
                        Schema.from("anotherSchemaName", Set.of("value1")),
                        Map.of("value1", "aValue")),
                arguments(
                        "mySchemaName",
                        "partition",
                        "PARTITION",
                        Schema.from("mySchemaName", Set.of("partition")),
                        Map.of("partition", "150")));
    }

    @ParameterizedTest
    @MethodSource("extractorFromSimpleExpression")
    public void shouldCreateAndExtractValuesSimpleExpression(
            String schemaName,
            String param,
            String expression,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor =
                extractor(String(), schemaName, param, expression);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEqualTo(expectedValues);
    }

    static Stream<Arguments> extractorArgumentsFromTemplateExpressions() {
        return Stream.of(
                arguments(
                        Template("prefix-#{name=VALUE}"),
                        Schema.from("prefix", Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        Template("aTemplate-#{value=VALUE,key=KEY}"),
                        Schema.from("aTemplate", Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Template("anotherTemplate-#{value1=VALUE,key1=KEY}"),
                        Schema.from("anotherTemplate", Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue")),
                arguments(
                        Template(
                                "mySchemaName-#{timestamp=TIMESTAMP,partition=PARTITION,topic=TOPIC}"),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")));
    }

    @ParameterizedTest
    @MethodSource("extractorArgumentsFromTemplateExpressions")
    public void shouldCreateAndExtractValuesFromTemplateExpressions(
            TemplateExpression expression,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), expression);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void shouldCreateAndExtractValuesFromNoExpressions() throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), TEST_SCHEMA);
        assertThat(extractor.schema()).isEqualTo(Schema.empty(TEST_SCHEMA));

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE"})
    public void shouldNotCreate(String expression) throws ExtractionException {
        assertThrows(
                ExtractionException.class,
                () ->
                        extractor(
                                TestSelectorSuppliers.JsonValue(),
                                TEST_SCHEMA,
                                Map.of("value", Expression(expression))));
    }
}
