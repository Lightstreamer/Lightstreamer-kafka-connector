
/*
 * Copyright (C) 2025 Lightstreamer Srl
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.staticFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.EMPTY_MAP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.test_utils.Records;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class FieldsExtractorTest {

    static Stream<Arguments> boundExtractionExpressions() {
        return Stream.of(
                arguments(EMPTY_MAP, false, false, EMPTY_MAP),
                arguments(
                        Map.of("name", Expression("VALUE")), true, false, Map.of("name", "aValue")),
                arguments(
                        Map.of("value", Expression("VALUE"), "key", Expression("KEY")),
                        false,
                        true,
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of("value", Expression("VALUE"), "key", Expression("KEY")),
                        true,
                        false,
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of(
                                "timestamp",
                                Expression("TIMESTAMP"),
                                "partition",
                                Expression("PARTITION"),
                                "topic",
                                Expression("TOPIC")),
                        false,
                        true,
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")),
                arguments(
                        Map.of(
                                "timestamp",
                                Expression("TIMESTAMP"),
                                "partition",
                                Expression("PARTITION"),
                                "topic",
                                Expression("TOPIC")),
                        false,
                        false,
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")),
                arguments(
                        Map.of(
                                "header1",
                                Expression("HEADERS[0]"),
                                "header2",
                                Expression("HEADERS['header-key2']")),
                        true,
                        false,
                        Map.of("header1", "header-value1", "header2", "header-value2")),
                arguments(
                        Map.of(
                                "header1",
                                Expression("HEADERS[0]"),
                                "header2",
                                Expression("HEADERS['header-key2']")),
                        true,
                        true,
                        Map.of("header1", "header-value1", "header2", "header-value2")));
    }

    @ParameterizedTest
    @MethodSource("boundExtractionExpressions")
    public void shouldExtractMapFromBoundExtractionExpressions(
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars,
            Map<String, String> expectedValues)
            throws ExtractionException {

        FieldsExtractor<String, String> extractor =
                DataExtractors.staticFieldsExtractor(
                        String(), expressions, skipOnFailure, mapNonScalars);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
        assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);

        Headers headers =
                new RecordHeaders()
                        .add("header-key1", "header-value1".getBytes())
                        .add("header-key2", "header-value2".getBytes());
        KafkaRecord<String, String> kafkaRecord =
                Records.recordWithHeaders("aKey", "aValue", headers);
        Map<String, String> values = extractor.extractMap(kafkaRecord);
        assertThat(values).isEqualTo(expectedValues);
        assertThat(extractor.mappedFields()).isEqualTo(expressions.keySet());
    }

    static Stream<Arguments> unboundExtractionExpressions() {
        return Stream.of(
                arguments(Collections.emptyList(), false, EMPTY_MAP),
                arguments(List.of(Expression("VALUE")), true, Map.of("VALUE", "aValue")),
                arguments(
                        List.of(Expression("VALUE"), Expression("KEY")),
                        false,
                        Map.of("KEY", "aKey", "VALUE", "aValue")),
                arguments(
                        List.of(Expression("VALUE"), Expression("KEY")),
                        true,
                        Map.of("VALUE", "aValue", "KEY", "aKey")),
                arguments(
                        List.of(
                                Expression("TIMESTAMP"),
                                Expression("PARTITION"),
                                Expression("TOPIC")),
                        false,
                        Map.of("PARTITION", "150", "TOPIC", "record-topic", "TIMESTAMP", "-1")),
                arguments(
                        List.of(Expression("HEADERS")),
                        true,
                        Map.of(
                                "header-key1",
                                "header-value1",
                                "header-key2",
                                "header-value2",
                                "header-index[0]",
                                "header-index-value1",
                                "header-index[1]",
                                "header-index-value2")));
    }

    @ParameterizedTest
    @MethodSource("unboundExtractionExpressions")
    public void shouldExtractMapFromAutoBoundExtractionExpressions(
            Collection<ExtractionExpression> expressions,
            boolean skipOnFailure,
            Map<String, String> expectedValues)
            throws ExtractionException {

        FieldsExtractor<String, String> extractor =
                DataExtractors.dynamicFieldsExtractor(String(), expressions, skipOnFailure);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
        assertThat(extractor.mapNonScalars()).isEqualTo(true);

        Headers headers =
                new RecordHeaders()
                        .add("header-key1", "header-value1".getBytes())
                        .add("header-key2", "header-value2".getBytes())
                        .add("header-index", "header-index-value1".getBytes())
                        .add("header-index", "header-index-value2".getBytes());
        KafkaRecord<String, String> kafkaRecord =
                Records.recordWithHeaders("aKey", "aValue", headers);
        Map<String, String> values = extractor.extractMap(kafkaRecord);
        assertThat(values).isEqualTo(expectedValues);
        assertThat(extractor.mappedFields()).isEmpty();
    }

    @Test
    public void shouldNotExtractMap() throws ExtractionException {
        FieldsExtractor<String, JsonNode> extractor =
                DataExtractors.staticFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        Map.of(
                                "undefined",
                                Expressions.Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Expressions.Wrapped("#{VALUE.name}")),
                        false,
                        false);

        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                extractor.extractMap(
                                        Records.record(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_attrib] not found");

        FieldsExtractor<String, JsonNode> autoBoundExtractor =
                DataExtractors.dynamicFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        List.of(
                                Expressions.Wrapped("#{VALUE.undefined_maps}"),
                                Expressions.Wrapped("#{VALUE.notes}")),
                        false);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                autoBoundExtractor.extractMap(
                                        Records.record(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_maps] not found");
    }

    @Test
    public void shouldNotExtractMapDueToNotMappingScalars()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        FieldsExtractor<String, JsonNode> extractor =
                staticFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        Map.of(
                                "complexObject",
                                Expressions.Wrapped("#{VALUE}"),
                                "simpleAttribute",
                                Expressions.Wrapped("#{VALUE.name}")),
                        false,
                        false);

        // Create the JSON message
        ObjectMapper om = new ObjectMapper();
        JsonNode message = om.readTree("{\"name\": \"joe\"}");

        // Extract the value from the Kafka Record
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> extractor.extractMap(Records.record("aValue", message)));
        assertThat(ve.getMessage())
                .contains("The expression [VALUE] must evaluate to a non-complex object");
    }

    @Test
    public void shouldSkipFailureExtraction() throws ExtractionException {
        // We expect that only the extraction related to the VALUE.undefined_attrib fails
        KafkaRecord<String, JsonNode> record =
                Records.record("aKey", SampleJsonNodeProvider().sampleMessage());

        FieldsExtractor<String, JsonNode> boundExtractor =
                DataExtractors.staticFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        Map.of(
                                "undefined",
                                Expressions.Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Expressions.Wrapped("#{VALUE.name}")),
                        true,
                        false);

        Map<String, String> tryExtractData = boundExtractor.extractMap(record);
        assertThat(tryExtractData).containsAtLeast("name", "joe");

        FieldsExtractor<String, JsonNode> autoBoundExtractor =
                DataExtractors.dynamicFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        List.of(
                                Expressions.Wrapped("#{VALUE.undefined_map}"),
                                Expressions.Wrapped("#{VALUE.notes}")),
                        true);
        assertThat(autoBoundExtractor.extractMap(record))
                .containsExactly(
                        "notes[0]", "note1",
                        "notes[1]", "note2");
    }

    @Test
    public void shouldExtractMapWithScalarMapping()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        FieldsExtractor<String, JsonNode> extractor =
                staticFieldsExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        Map.of(
                                "complexObject",
                                Expressions.Wrapped("#{VALUE}"),
                                "simpleAttribute",
                                Expressions.Wrapped("#{VALUE.name}")),
                        false,
                        true);

        // Create the JSON message
        ObjectMapper om = new ObjectMapper();
        JsonNode message = om.readTree("{\"name\": \"joe\"}");

        // Extract the value from the Kafka Record
        KafkaRecord<String, JsonNode> record = Records.record("aValue", message);
        Map<String, String> tryExtractData = extractor.extractMap(record);

        // Ensure that both the complex object and the simple attribute are extracted correctly
        assertThat(tryExtractData)
                .containsExactly("complexObject", message.toString(), "simpleAttribute", "joe");
    }

    @Test
    public void shouldNotCreateExtractorDueToExtractionException() {
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () ->
                                DataExtractors.staticFieldsExtractor(
                                        TestSelectorSuppliers.JsonValue(),
                                        Map.of("value", Expression("VALUE.a. .b")),
                                        false,
                                        false));
        assertThat(ee.getMessage())
                .contains("Found the invalid expression [VALUE.a. .b] with missing tokens");

        ExtractionException ee2 =
                assertThrows(
                        ExtractionException.class,
                        () ->
                                DataExtractors.dynamicFieldsExtractor(
                                        TestSelectorSuppliers.JsonValue(),
                                        List.of(Expression("VALUE.map[invalid-index]")),
                                        false));
        assertThat(ee2.getMessage())
                .contains("Found the invalid indexed expression [VALUE.map[invalid-index]]");
    }
}
