
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.canonicalItemExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.EmptyTemplate;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.Records;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

public class CanonicalItemExtractorTest {

    static final String TEST_SCHEMA = "schema";

    @Test
    public void shouldBuildEqualExtractors() throws ExtractionException {
        CanonicalItemExtractor<String, String> extractor1 =
                canonicalItemExtractor(String(), Template("prefix1-#{aKey=KEY,aValue=VALUE}"));

        assertThat(extractor1.equals(extractor1)).isTrue();

        CanonicalItemExtractor<String, String> extractor2 =
                canonicalItemExtractor(String(), Template("prefix1-#{aValue=VALUE,aKey=KEY}"));
        assertThat(extractor1.hashCode()).isEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isTrue();
    }

    @Test
    public void shouldBuildNotEqualExtractors() throws ExtractionException {
        CanonicalItemExtractor<String, String> extractor1 =
                canonicalItemExtractor(String(), Template("prefix1-#{aKey=KEY}"));
        assertThat(extractor1.equals(extractor1)).isTrue();

        CanonicalItemExtractor<String, String> extractor2 =
                canonicalItemExtractor(String(), Template("prefix2-#{aKey=KEY}"));
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isFalse();

        CanonicalItemExtractor<String, String> extractor3 =
                canonicalItemExtractor(String(), Template("prefix1-#{aValue=VALUE}"));
        assertThat(extractor1.hashCode()).isNotEqualTo(extractor3.hashCode());
        assertThat(extractor1.equals(extractor3)).isFalse();

        assertThat(extractor2.hashCode()).isNotEqualTo(extractor3.hashCode());
        assertThat(extractor2.equals(extractor3)).isFalse();

        // Verify with different value selector type while using the same template
        CanonicalItemExtractor<String, JsonNode> extractor4 =
                canonicalItemExtractor(JsonValue(), Template("prefix1-#{aValue=VALUE}"));
        assertThat(extractor3.hashCode()).isNotEqualTo(extractor4.hashCode());
        assertThat(extractor3.equals(extractor4)).isFalse();
    }

    static Stream<Arguments> extractorArgumentsFromTemplateExpressions() {
        return Stream.of(
                arguments(
                        Template("prefix-#{name=VALUE}"),
                        Schema.from("prefix", Set.of("name")),
                        "prefix-[name=aValue]"),
                arguments(
                        Template("aTemplate-#{value=VALUE,key=KEY}"),
                        Schema.from("aTemplate", Set.of("value", "key")),
                        "aTemplate-[key=aKey,value=aValue]"),
                arguments(
                        Template("anotherTemplate-#{value1=VALUE,key1=KEY}"),
                        Schema.from("anotherTemplate", Set.of("value1", "key1")),
                        "anotherTemplate-[key1=aKey,value1=aValue]"),
                arguments(
                        Template(
                                "mySchemaName-#{timestamp=TIMESTAMP,partition=PARTITION,topic=TOPIC}"),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "topic")),
                        "mySchemaName-[partition=150,timestamp=-1,topic=record-topic]"),
                arguments(
                        Template(
                                "mySchemaName-#{timestamp=TIMESTAMP,partition=PARTITION,headers=HEADERS[0]}"),
                        Schema.from("mySchemaName", Set.of("timestamp", "partition", "headers")),
                        "mySchemaName-[headers=header-value1,partition=150,timestamp=-1]"));
    }

    @ParameterizedTest
    @MethodSource("extractorArgumentsFromTemplateExpressions")
    public void shouldCreateAndExtractCanonicalItemFromFromTemplateExpressions(
            TemplateExpression templateExpression,
            Schema expectedSchema,
            String expectedCompactedString)
            throws ExtractionException {
        CanonicalItemExtractor<String, String> extractor =
                canonicalItemExtractor(String(), templateExpression);
        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        Headers headers = new RecordHeaders().add("header-key1", "header-value1".getBytes());
        KafkaRecord<String, String> kafkaRecord =
                Records.recordWithHeaders("aKey", "aValue", headers);

        assertThat(extractor.extractCanonicalItem(kafkaRecord)).isEqualTo(expectedCompactedString);
    }

    @Test
    public void shouldCreateAndExtractCanonicalItemFromSimpleItem() throws ExtractionException {
        CanonicalItemExtractor<String, String> extractor =
                canonicalItemExtractor(String(), EmptyTemplate(TEST_SCHEMA));
        assertThat(extractor.schema()).isEqualTo(Schema.empty(TEST_SCHEMA));

        KafkaRecord<String, String> kafkaRecord = Records.record("aKey", "aValue");
        assertThat(extractor.extractCanonicalItem(kafkaRecord)).isEqualTo(TEST_SCHEMA);
    }

    @Test
    public void shouldFailExtraction() throws ExtractionException {
        CanonicalItemExtractor<String, JsonNode> extractor =
                canonicalItemExtractor(
                        TestSelectorSuppliers.JsonValue(),
                        Template("aTemplate-#{value=VALUE.undefined_attrib,key=KEY}"));

        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                extractor.extractCanonicalItem(
                                        Records.record(
                                                "aKey", SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [undefined_attrib] not found");
    }

    @Test
    public void shouldNotCreateDueToExtractionException() {
        assertThrows(
                ExtractionException.class,
                () ->
                        canonicalItemExtractor(
                                TestSelectorSuppliers.JsonValue(),
                                Template("aTemplate-#{aValue=VALUE[]}")));
    }
}
