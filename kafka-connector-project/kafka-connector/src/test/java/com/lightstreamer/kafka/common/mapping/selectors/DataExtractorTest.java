
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.EMPTY_MAP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Records;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false, false);
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false, false);
        assertThat(extractor1.hashCode()).isEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isTrue();
    }

    @Test
    public void shouldBuildNotEqualExtractors() throws ExtractionException {
        DataExtractor<String, String> extractor1 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), true, false);
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY")), false, false);
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isFalse();

        extractor2 =
                extractor(String(), "prefix1", Map.of("aKey1", Expression("KEY")), true, false);
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isFalse();
    }

    static Stream<Arguments> extractorFromExtractionExpressions() {
        return Stream.of(
                arguments(
                        TEST_SCHEMA,
                        EMPTY_MAP,
                        false,
                        false,
                        Schema.empty(TEST_SCHEMA),
                        EMPTY_MAP,
                        TEST_SCHEMA),
                arguments(
                        TEST_SCHEMA,
                        Map.of("name", Expression("VALUE")),
                        true,
                        false,
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue"),
                        TEST_SCHEMA + "-[name=aValue]"),
                arguments(
                        "aSchemaName",
                        Map.of("value", Expression("VALUE"), "key", Expression("KEY")),
                        false,
                        true,
                        Schema.from("aSchemaName", Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue"),
                        "aSchemaName-[key=aKey,value=aValue]"),
                arguments(
                        "anotherSchemaName",
                        Map.of("value1", Expression("VALUE"), "key1", Expression("KEY")),
                        true,
                        false,
                        Schema.from("anotherSchemaName", Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue"),
                        "anotherSchemaName-[key1=aKey,value1=aValue]"),
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
                        true,
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1"),
                        "mySchemaName-[partition=150,timestamp=-1,topic=record-topic]"),
                arguments(
                        "mySchemaName",
                        Map.of(
                                "header1",
                                Expression("HEADERS[0]"),
                                "header2",
                                Expression("HEADERS['header-key2']")),
                        true,
                        false,
                        Schema.from("mySchemaName", Set.of("header1", "header2")),
                        Map.of("header1", "header-value1", "header2", "header-value2"),
                        "mySchemaName-[header1=header-value1,header2=header-value2]"));
    }

    @ParameterizedTest
    @MethodSource("extractorFromExtractionExpressions")
    public void shouldCreateAndExtractValuesFromExtractionExpressions(
            String schemaName,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars,
            Schema expectedSchema,
            Map<String, String> expectedValues,
            String expectedCompactedString)
            throws ExtractionException {
        DataExtractor<String, String> extractor =
                extractor(String(), schemaName, expressions, skipOnFailure, mapNonScalars);
        Schema schema = extractor.schema();
        assertThat(schema).isEqualTo(expectedSchema);
        assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
        assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);

        Headers headers =
                new RecordHeaders()
                        .add("header-key1", "header-value1".getBytes())
                        .add("header-key2", "header-value2".getBytes());
        KafkaRecord<String, String> kafkaRecord =
                Records.KafkaRecordWithHeaders("aKey", "aValue", headers);
        Map<String, String> values = extractor.extractAsMap(kafkaRecord);
        assertThat(values).isEqualTo(expectedValues);
        assertThat(extractor.extractAsCanonicalItem(kafkaRecord))
                .isEqualTo(expectedCompactedString);
    }

    static Stream<Arguments> extractorArgumentsFromTemplateExpressions() {
        return Stream.of(
                arguments(
                        Template("prefix-#{name=VALUE}"),
                        Schema.from("prefix", Set.of("name")),
                        Map.of("name", "aValue"),
                        "prefix-[name=aValue]"),
                arguments(
                        Template("aTemplate-#{value=VALUE,key=KEY}"),
                        Schema.from("aTemplate", Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue"),
                        "aTemplate-[key=aKey,value=aValue]"),
                arguments(
                        Template("anotherTemplate-#{value1=VALUE,key1=KEY}"),
                        Schema.from("anotherTemplate", Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue"),
                        "anotherTemplate-[key1=aKey,value1=aValue]"),
                arguments(
                        Template(
                                "mySchemaName-#{timestamp=TIMESTAMP,partition=PARTITION,topic=TOPIC}"),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1"),
                        "mySchemaName-[partition=150,timestamp=-1,topic=record-topic]"),
                arguments(
                        Template(
                                "mySchemaName-#{timestamp=TIMESTAMP,partition=PARTITION,headers=HEADERS[0]}"),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "headers")),
                        Map.of("partition", "150", "timestamp", "-1", "headers", "header-value1"),
                        "mySchemaName-[headers=header-value1,partition=150,timestamp=-1]"));
    }

    @ParameterizedTest
    @MethodSource("extractorArgumentsFromTemplateExpressions")
    public void shouldCreateAndExtractValuesFromTemplateExpressions(
            TemplateExpression templateExpression,
            Schema expectedSchema,
            Map<String, String> expectedValues,
            String expectedCompactedString)
            throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), templateExpression);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);
        assertThat(extractor.skipOnFailure()).isFalse();
        assertThat(extractor.mapNonScalars()).isFalse();

        Headers headers = new RecordHeaders().add("header-key1", "header-value1".getBytes());
        KafkaRecord<String, String> kafkaRecord =
                Records.KafkaRecordWithHeaders("aKey", "aValue", headers);

        Map<String, String> values = extractor.extractAsMap(kafkaRecord);
        assertThat(values).isEqualTo(expectedValues);
        assertThat(extractor.extractAsCanonicalItem(kafkaRecord))
                .isEqualTo(expectedCompactedString);
    }

    @Test
    public void shouldCreateAndExtractValuesFromNoExpressions() throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(String(), TEST_SCHEMA);
        assertThat(extractor.schema()).isEqualTo(Schema.empty(TEST_SCHEMA));
        assertThat(extractor.skipOnFailure()).isFalse();
        assertThat(extractor.mapNonScalars()).isFalse();

        KafkaRecord<String, String> kafkaRecord = Records.KafkaRecord("aKey", "aValue");
        Map<String, String> values = extractor.extractAsMap(kafkaRecord);
        assertThat(values).isEmpty();
        assertThat(extractor.extractAsCanonicalItem(kafkaRecord)).isEqualTo(TEST_SCHEMA);
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
                        false,
                        false);

        // We expect that the whole extraction fails
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                extractor.extractAsMap(
                                        Records.KafkaRecord(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_attrib] not found");

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                extractor.extractAsCanonicalItem(
                                        Records.KafkaRecord(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_attrib] not found");
    }

    @Test
    public void shouldSkipFailureExtractionAsMap() throws ExtractionException {
        DataExtractor<String, JsonNode> extractor =
                extractor(
                        TestSelectorSuppliers.JsonValue(),
                        "fields",
                        Map.of(
                                "undefined",
                                Expressions.Wrapped("#{VALUE.undefined_attrib}"),
                                "name",
                                Expressions.Wrapped("#{VALUE.name}")),
                        true,
                        false);

        // We expect that only the extraction related to the VALUE.undefined_attrib fails
        KafkaRecord<String, JsonNode> record =
                Records.KafkaRecord("aKey", SampleJsonNodeProvider().sampleMessage());
        Map<String, String> tryExtractData = extractor.extractAsMap(record);
        assertThat(tryExtractData).containsAtLeast("name", "joe");
    }

    @Test
    public void shouldMapNonScalarValues()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        DataExtractor<String, JsonNode> extractor =
                extractor(
                        TestSelectorSuppliers.JsonValue(),
                        "fields",
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
        KafkaRecord<String, JsonNode> record = Records.KafkaRecord("aValue", message);
        Map<String, String> tryExtractData = extractor.extractAsMap(record);

        // Ensure that both the complex object and the simple attribute are extracted correctly
        assertThat(tryExtractData)
                .containsExactly("complexObject", message.toString(), "simpleAttribute", "joe");
        assertThat(extractor.extractAsCanonicalItem(record))
                .isEqualTo("fields-[complexObject={\"name\":\"joe\"},simpleAttribute=joe]");
    }

    @Test
    public void shouldFailMappingNonScalarValues()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        DataExtractor<String, JsonNode> extractor =
                extractor(
                        TestSelectorSuppliers.JsonValue(),
                        "fields",
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
                        () -> extractor.extractAsMap(Records.KafkaRecord("aValue", message)));
        assertThat(ve.getMessage())
                .contains("The expression [VALUE] must evaluate to a non-complex object");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.a. .b"})
    public void shouldNotCreateDueToExtractionException(String expression) {
        assertThrows(
                ExtractionException.class,
                () ->
                        extractor(
                                TestSelectorSuppliers.JsonValue(),
                                TEST_SCHEMA,
                                Map.of("value", Expression(expression)),
                                false,
                                false));
    }

    //     @Test
    //     public void test() throws ExtractionException, JsonMappingException,
    // JsonProcessingException {

    //         FieldConfigs f = FieldConfigs.from(Map.of("pippo", "#{VALUE.pippo}"));
    //         var extractor = f.extractor(TestSelectorSuppliers.JsonValue(), true);

    //         TopicConfigurations topicsConfig =
    //                 TopicConfigurations.of(
    //                         ItemTemplateConfigs.empty(),
    //                         List.of(
    //                                 TopicMappingConfig.fromDelimitedMappings(
    //                                         "record-topic", "simple-item")));

    //         ItemTemplates<String, JsonNode> templates =
    //                 Items.templatesFrom(topicsConfig, TestSelectorSuppliers.JsonValue());

    //         RecordMapper<String, JsonNode> mapper =
    //                 RecordMapper.<String, JsonNode>builder()
    //                         .withFieldExtractor(extractor)
    //                         .withTemplateExtractors(templates.extractorsByTopicName())
    //                         .build();

    //         ObjectMapper oo = new ObjectMapper();
    //         String content =
    //                 """
    //                         {
    //                             "pippo":"pluto",
    //                             "name":"pippo"
    //                         }
    //                         """;
    //         JsonNode tree = oo.readTree(content);
    //         KafkaRecord<String, JsonNode> record = Records.record("key", tree);
    //         MappedRecord map = mapper.map(record);
    //         System.out.println(map);
    //     }
}
