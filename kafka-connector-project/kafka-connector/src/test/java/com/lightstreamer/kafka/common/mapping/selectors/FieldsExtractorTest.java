
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.namedFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedWithWildcards;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecord;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecordWithHeaders;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Json;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class FieldsExtractorTest {

    static Stream<Arguments> namedFieldsExpressions() {
        return Stream.of(
                arguments(
                        Map.of("name", Wrapped("#{VALUE}")), true, false, Map.of("name", "aValue")),
                arguments(
                        Map.of("value", Wrapped("#{VALUE}"), "key", Wrapped("#{KEY}")),
                        false,
                        true,
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of("value", Wrapped("#{VALUE}"), "key", Wrapped("#{KEY}")),
                        true,
                        false,
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of(
                                "timestamp",
                                Wrapped("#{TIMESTAMP}"),
                                "partition",
                                Wrapped("#{PARTITION}"),
                                "topic",
                                Wrapped("#{TOPIC}")),
                        false,
                        true,
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")),
                arguments(
                        Map.of(
                                "timestamp",
                                Wrapped("#{TIMESTAMP}"),
                                "partition",
                                Wrapped("#{PARTITION}"),
                                "topic",
                                Wrapped("#{TOPIC}")),
                        false,
                        false,
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")),
                arguments(
                        Map.of(
                                "header1",
                                Wrapped("#{HEADERS[0]}"),
                                "header2",
                                Wrapped("#{HEADERS['header-key2']}")),
                        true,
                        false,
                        Map.of("header1", "header-value1", "header2", "header-value2")),
                arguments(
                        Map.of(
                                "header1",
                                Wrapped("#{HEADERS[0]}"),
                                "header2",
                                Wrapped("#{HEADERS['header-key2']}")),
                        true,
                        true,
                        Map.of("header1", "header-value1", "header2", "header-value2")));
    }

    @ParameterizedTest
    @MethodSource("namedFieldsExpressions")
    public void shouldExtractMapFromNamedFieldsExpressions(
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars,
            Map<String, String> expectedValues)
            throws ExtractionException {

        FieldsExtractor<String, String> extractor =
                DataExtractors.namedFieldsExtractor(
                        String(), expressions, skipOnFailure, mapNonScalars);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
        assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);

        Headers headers =
                new RecordHeaders()
                        .add("header-key1", "header-value1".getBytes())
                        .add("header-key2", "header-value2".getBytes());
        KafkaRecord<String, String> kafkaRecord = KafkaRecordWithHeaders("aKey", "aValue", headers);
        Map<String, String> values = extractor.extractMap(kafkaRecord);
        assertThat(values).isEqualTo(expectedValues);
        assertThat(extractor.mappedFields()).isEqualTo(expressions.keySet());
    }

    static Stream<Arguments> discoveredFieldsExpressions() {
        return Stream.of(
                arguments(
                        List.of(WrappedWithWildcards("#{VALUE.*}")),
                        true,
                        Map.of("name", "joe", "signature", "YWJjZA==")),
                arguments(
                        List.of(WrappedWithWildcards("#{KEY.*}")),
                        true,
                        Map.of("name", "joe", "signature", "YWJjZA==")),
                arguments(
                        List.of(
                                WrappedWithWildcards("#{VALUE.notes.*}"),
                                WrappedWithWildcards("#{VALUE.*}")),
                        false,
                        Map.of(
                                "notes[0]",
                                "note1",
                                "notes[1]",
                                "note2",
                                "name",
                                "joe",
                                "signature",
                                "YWJjZA==")),
                arguments(
                        List.of(
                                WrappedWithWildcards("#{VALUE.*}"),
                                WrappedWithWildcards("#{HEADERS.*}")),
                        true,
                        Map.of(
                                "name",
                                "joe",
                                "signature",
                                "YWJjZA==",
                                "header-key1",
                                "header-value1",
                                "header-key2",
                                "header-value2",
                                "header-index[0]",
                                "header-index-value1",
                                "header-index[1]",
                                "header-index-value2")),
                arguments(
                        List.of(WrappedWithWildcards("#{HEADERS['header-index'].*}")),
                        false,
                        Map.of(
                                "header-index[0]",
                                "header-index-value1",
                                "header-index[1]",
                                "header-index-value2")),
                arguments(
                        List.of(WrappedWithWildcards("#{HEADERS.header-index.*}")),
                        false,
                        Map.of(
                                "header-index[0]",
                                "header-index-value1",
                                "header-index[1]",
                                "header-index-value2")));
    }

    @ParameterizedTest
    @MethodSource("discoveredFieldsExpressions")
    public void shouldExtractMapFromDiscoveredFieldsExpressions(
            Collection<ExtractionExpression> expressions,
            boolean skipOnFailure,
            Map<String, String> expectedValues)
            throws ExtractionException {

        FieldsExtractor<JsonNode, JsonNode> extractor =
                DataExtractors.discoveredFieldsExtractor(Json(), expressions, skipOnFailure);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
        assertThat(extractor.mapNonScalars()).isEqualTo(true);

        Headers headers =
                new RecordHeaders()
                        .add("header-key1", "header-value1".getBytes())
                        .add("header-key2", "header-value2".getBytes())
                        .add("header-index", "header-index-value1".getBytes())
                        .add("header-index", "header-index-value2".getBytes());

        JsonNode sampleMessage = SampleJsonNodeProvider().sampleMessage();
        KafkaRecord<JsonNode, JsonNode> kafkaRecord =
                KafkaRecordWithHeaders(sampleMessage, sampleMessage, headers);
        Map<String, String> values = extractor.extractMap(kafkaRecord);
        assertThat(values).containsAtLeastEntriesIn(expectedValues);
        assertThat(extractor.mappedFields()).isEmpty();
    }

    @Test
    public void shouldExtractMapFromComposedExtractor() throws ExtractionException {
        // Prepare the named and discovered extraction expressions
        Map<String, ExtractionExpression> namedExpressions =
                Map.of(
                        "name", Wrapped("#{VALUE.name}"),
                        "signature", Wrapped("#{VALUE.signature}"),
                        "conflictingKey", Wrapped("#{VALUE.name}"),
                        "key", Wrapped("#{KEY}"),
                        "topic", Wrapped("#{TOPIC}"));
        List<ExtractionExpression> discoveredExpressions =
                List.of(
                        WrappedWithWildcards("#{VALUE.notes.*}"),
                        WrappedWithWildcards("#{HEADERS.*}"));

        // Create the composed FieldsExtractor
        FieldsExtractor<String, JsonNode> namedExtractor =
                DataExtractors.namedFieldsExtractor(JsonValue(), namedExpressions, false, false);
        FieldsExtractor<String, JsonNode> discoveredExtractor =
                DataExtractors.discoveredFieldsExtractor(JsonValue(), discoveredExpressions, false);

        FieldsExtractor<String, JsonNode> composedExtractor =
                DataExtractors.composedFieldsExtractor(
                        List.of(discoveredExtractor, namedExtractor));
        // Extract the values from a Kafka Record
        Headers headers = new RecordHeaders();
        headers.add("conflictingKey", "header-value1".getBytes(StandardCharsets.UTF_8));
        headers.add("accountId", "12345".getBytes(StandardCharsets.UTF_8));
        headers.add("accountId", "67890".getBytes(StandardCharsets.UTF_8));

        KafkaRecord<String, JsonNode> kafkaRecord =
                KafkaRecordWithHeaders("aKey", SampleJsonNodeProvider().sampleMessage(), headers);
        Map<String, String> values = composedExtractor.extractMap(kafkaRecord);

        // Verify the extracted values
        assertThat(values)
                .containsExactly(
                        "name", "joe",
                        "key", "aKey",
                        "conflictingKey",
                                "joe", // The bound extractor has precedence over the auto-bound one
                        "topic", "record-topic",
                        "notes[0]", "note1",
                        "notes[1]", "note2",
                        "signature", "YWJjZA==",
                        "accountId[0]", "12345",
                        "accountId[1]", "67890");
    }

    static Stream<Arguments> invalidExpressionsForNamedFieldsExtractor() {
        Map<String, Expressions.ExtractionExpression> mapWithNullEntries =
                new java.util.HashMap<>();
        mapWithNullEntries.put("field1", Wrapped("#{VALUE.attrib}"));
        mapWithNullEntries.put("field2", null);
        mapWithNullEntries.put("field3", Wrapped("#{KEY}"));
        mapWithNullEntries.put(null, WrappedWithWildcards("#{VALUE.*}"));

        return Stream.of(
                arguments(Collections.emptyMap()),
                arguments((Map<String, ExtractionExpression>) null),
                arguments(mapWithNullEntries),
                arguments(
                        Map.of(
                                "field",
                                WrappedWithWildcards("#{VALUE.attrib.*}"),
                                "field2",
                                WrappedWithWildcards("#{VALUE.[0].*}"))),
                arguments(
                        Map.of(
                                "field",
                                Wrapped("#{VALUE.attrib}"),
                                "field2",
                                WrappedWithWildcards("#{KEY.*}"))));
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionsForNamedFieldsExtractor")
    public void shouldNotCreateNamedFieldsExtractor(Map<String, ExtractionExpression> expressions) {
        IllegalArgumentException ee =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DataExtractors.namedFieldsExtractor(
                                        JsonValue(), expressions, false, false));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo("All expressions must be non-wildcard expressions");
    }

    static Stream<Arguments> invalidExpressionsForDiscoveredFieldsExtractor() {
        List<ExtractionExpression> listWithNullElements = new ArrayList<>();
        listWithNullElements.add(WrappedWithWildcards("#{VALUE.attrib.*}"));
        listWithNullElements.add(null);
        listWithNullElements.add(WrappedWithWildcards("#{KEY.*}"));

        return Stream.of(
                arguments(Collections.emptyList()),
                arguments((List<ExtractionExpression>) null),
                arguments(listWithNullElements),
                arguments(List.of(Wrapped("#{VALUE.attrib}"), Wrapped("#{TIMESTAMP}"))),
                arguments(List.of(WrappedWithWildcards("#{VALUE.attrib.*}"), Wrapped("#{KEY}"))));
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionsForDiscoveredFieldsExtractor")
    public void shouldNotCreateDiscoveredFieldsExtractor(List<ExtractionExpression> expressions) {
        IllegalArgumentException ee =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DataExtractors.discoveredFieldsExtractor(
                                        JsonValue(), expressions, false));
        assertThat(ee).hasMessageThat().isEqualTo("All expressions must be wildcard expressions");
    }

    static Stream<Arguments> invalidComposedExtractorArguments() {
        return Stream.of(
                arguments((List<FieldsExtractor<String, JsonNode>>) null),
                arguments(Collections.emptyList()));
    }

    @ParameterizedTest
    @MethodSource("invalidComposedExtractorArguments")
    public void shouldNotCreateComposedExtractorWithNoExtractors(
            List<FieldsExtractor<String, JsonNode>> extractors) {
        IllegalArgumentException iae =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> DataExtractors.composedFieldsExtractor(extractors));
        assertThat(iae).hasMessageThat().isEqualTo("Extractors list must not be null or empty");
    }

    @Test
    public void shouldNotCreateExtractorDueToExtractionException() {
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () ->
                                DataExtractors.discoveredFieldsExtractor(
                                        JsonValue(),
                                        List.of(
                                                WrappedWithWildcards(
                                                        "#{VALUE.map[invalid-index].*}")),
                                        false));
        assertThat(ee)
                .hasMessageThat()
                .contains("Found the invalid indexed expression [VALUE.map[invalid-index].*]");

        ee =
                assertThrows(
                        ExtractionException.class,
                        () ->
                                DataExtractors.namedFieldsExtractor(
                                        JsonValue(),
                                        Map.of("field", Wrapped("#{VALUE.map[invalid-index]}")),
                                        false,
                                        true));
        assertThat(ee)
                .hasMessageThat()
                .contains("Found the invalid indexed expression [VALUE.map[invalid-index]]");
    }

    @Test
    public void shouldNotExtractMap() throws ExtractionException {
        FieldsExtractor<String, JsonNode> namedExtractor =
                DataExtractors.namedFieldsExtractor(
                        JsonValue(),
                        Map.of(
                                "undefined",
                                Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Wrapped("#{VALUE.name}")),
                        false,
                        false);

        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                namedExtractor.extractMap(
                                        KafkaRecord(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_attrib] not found");

        FieldsExtractor<String, JsonNode> discoveredExtractor =
                DataExtractors.discoveredFieldsExtractor(
                        JsonValue(),
                        List.of(
                                WrappedWithWildcards("#{VALUE.undefined_maps.*}"),
                                WrappedWithWildcards("#{VALUE.notes.*}")),
                        false);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                discoveredExtractor.extractMap(
                                        KafkaRecord(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_maps] not found");

        FieldsExtractor<String, JsonNode> composExtractor =
                DataExtractors.composedFieldsExtractor(
                        List.of(discoveredExtractor, namedExtractor));
        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                composExtractor.extractMap(
                                        KafkaRecord(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        ;
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_maps] not found");
    }

    @Test
    public void shouldNotExtractMapDueToNotMappingScalars()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        FieldsExtractor<String, JsonNode> extractor =
                namedFieldsExtractor(
                        JsonValue(),
                        Map.of(
                                "complexObject",
                                Wrapped("#{VALUE}"),
                                "simpleAttribute",
                                Wrapped("#{VALUE.name}")),
                        false,
                        false);

        // Create the JSON message
        ObjectMapper om = new ObjectMapper();
        JsonNode message = om.readTree("{\"name\": \"joe\"}");

        // Extract the value from the Kafka Record
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> extractor.extractMap(KafkaRecord("aValue", message)));
        assertThat(ve)
                .hasMessageThat()
                .contains("The expression [VALUE] must evaluate to a non-complex object");
    }

    @Test
    public void shouldSkipFailureExtraction() throws ExtractionException {
        // We expect that only the extraction related to the VALUE.undefined_attrib fails
        KafkaRecord<String, JsonNode> record =
                KafkaRecord("aKey", SampleJsonNodeProvider().sampleMessage());

        FieldsExtractor<String, JsonNode> namedExtractor =
                DataExtractors.namedFieldsExtractor(
                        JsonValue(),
                        Map.of(
                                "undefined",
                                Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Wrapped("#{VALUE.name}")),
                        true,
                        false);

        Map<String, String> tryExtractData = namedExtractor.extractMap(record);
        assertThat(tryExtractData).containsAtLeast("name", "joe");

        // We expect that only the extraction related to the VALUE.undefined_map fails
        FieldsExtractor<String, JsonNode> discoveredExtractor =
                DataExtractors.discoveredFieldsExtractor(
                        JsonValue(),
                        List.of(
                                WrappedWithWildcards("#{VALUE.undefined_map.*}"),
                                WrappedWithWildcards("#{VALUE.notes.*}")),
                        true);
        assertThat(discoveredExtractor.extractMap(record))
                .containsExactly(
                        "notes[0]", "note1",
                        "notes[1]", "note2");
    }

    @Test
    public void shouldExtractMapWithScalarMapping()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        FieldsExtractor<String, JsonNode> extractor =
                namedFieldsExtractor(
                        JsonValue(),
                        Map.of(
                                "complexObject",
                                Wrapped("#{VALUE}"),
                                "simpleAttribute",
                                Wrapped("#{VALUE.name}")),
                        false,
                        true);

        // Create the JSON message
        ObjectMapper om = new ObjectMapper();
        JsonNode message = om.readTree("{\"name\": \"joe\"}");

        // Extract the value from the Kafka Record
        KafkaRecord<String, JsonNode> record = KafkaRecord("aValue", message);
        Map<String, String> tryExtractData = extractor.extractMap(record);

        // Ensure that both the complex object and the simple attribute are extracted correctly
        assertThat(tryExtractData)
                .containsExactly("complexObject", message.toString(), "simpleAttribute", "joe");
    }
}
