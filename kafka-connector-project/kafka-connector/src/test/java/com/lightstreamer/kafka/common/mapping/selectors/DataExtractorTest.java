
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.Records;
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
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false);
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false);
        assertThat(extractor1.hashCode()).isEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isTrue();
    }

    @Test
    public void shouldBuildNotEqualExtractors() throws ExtractionException {
        DataExtractor<String, String> extractor1 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), true);
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false);
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isFalse();

        extractor2 = extractor(String(), "prefix1", Map.of("aKey1", Expression("KEY")), true);
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isFalse();
    }

    static Stream<Arguments> extractorFromExtractionExpressions() {
        return Stream.of(
                arguments(TEST_SCHEMA, EMPTY_MAP, false, Schema.empty(TEST_SCHEMA), EMPTY_MAP),
                arguments(
                        TEST_SCHEMA,
                        Map.of("name", Expression("VALUE")),
                        true,
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        "aSchemNAme",
                        Map.of("value", Expression("VALUE"), "key", Expression("KEY")),
                        false,
                        Schema.from("aSchemNAme", Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        "anotherSchemaName",
                        Map.of("value1", Expression("VALUE"), "key1", Expression("KEY")),
                        true,
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
                        false,
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")));
    }

    @ParameterizedTest
    @MethodSource("extractorFromExtractionExpressions")
    public void shouldCreateAndExtractValuesExtractionExpressions(
            String schemaName,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor =
                extractor(String(), schemaName, expressions, skipOnFailure);
        Schema schema = extractor.schema();
        assertThat(schema).isEqualTo(expectedSchema);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);

        KafkaRecord<String, String> kafkaRecord = Records.record("aKey", "aValue");
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
            TemplateExpression templateExpression,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), templateExpression);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);
        assertThat(extractor.skipOnFailure()).isFalse();

        KafkaRecord<String, String> kafkaRecord = Records.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void shouldCreateAndExtractValuesFromNoExpressions() throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), TEST_SCHEMA);
        assertThat(extractor.schema()).isEqualTo(Schema.empty(TEST_SCHEMA));
        assertThat(extractor.skipOnFailure()).isFalse();

        KafkaRecord<String, String> kafkaRecord = Records.record("aKey", "aValue");
        SchemaAndValues schemaAndValues = extractor.extractData(kafkaRecord);
        Map<String, String> values = schemaAndValues.values();
        assertThat(values).isEmpty();
    }

    @Test
    public void shouldFailExtraction() throws ExtractionException {
        DataExtractor<String, JsonNode> extractor =
                extractor(
                        TestSelectorSuppliers.JsonValue(),
                        "fields",
                        Map.of(
                                "undefined",
                                Expressions.Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Expressions.Wrapped("#{VALUE.name}")),
                        false);

        // We expect that the whole extraction fails
        assertThrows(
                ValueException.class,
                () -> extractor.extractData(Records.record("aKey", JsonNodeProvider.RECORD)));
    }

    @Test
    public void shouldSkipFailurelExtraction() throws ExtractionException {
        DataExtractor<String, JsonNode> extractor =
                extractor(
                        TestSelectorSuppliers.JsonValue(),
                        "fields",
                        Map.of(
                                "undefined",
                                Expressions.Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Expressions.Wrapped("#{VALUE.name}")),
                        true);

        // We expect that only the extraction related to the VALUE.undefined_attrib fails
        SchemaAndValues tryExtractData =
                extractor.extractData(Records.record("aKey", JsonNodeProvider.RECORD));
        assertThat(tryExtractData.values()).containsAtLeast("name", "joe");
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
                                Map.of("value", Expression(expression)),
                                false));
    }
}
