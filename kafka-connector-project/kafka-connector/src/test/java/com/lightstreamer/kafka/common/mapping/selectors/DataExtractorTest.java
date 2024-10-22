
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
import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.emptyMap;

import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DataExtractorTest {

    static final String TEST_SCHEMA = "schema";

    private static DataExtractor<String, String> extractor(
            String schemaName, Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        return DataExtractor.<String, String>builder()
                .withSuppliers(TestSelectorSuppliers.String())
                .withSchemaName(schemaName)
                .withExpressions(expressions)
                .build();
    }

    @Test
    public void shouldCreateEqualExtractors() throws ExtractionException {
        DataExtractor<String, String> extractor1 =
                extractor("prefix1", Map.of("aKey", Expression("KEY")));
        assertThat(extractor1.equals(extractor1)).isTrue();

        DataExtractor<String, String> extractor2 =
                extractor("prefix1", Map.of("aKey", Expression("KEY")));
        assertThat(extractor1.hashCode()).isEqualTo(extractor2.hashCode());
        assertThat(extractor1.equals(extractor2)).isTrue();
    }

    static Stream<Arguments> stringExtractorArguments() {
        return Stream.of(
                arguments(emptyMap(), Schema.empty(TEST_SCHEMA), emptyMap()),
                arguments(
                        Map.of("name", Expressions.Expression("VALUE")),
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        Map.of(
                                "value",
                                Expressions.Expression("VALUE"),
                                "key",
                                Expressions.Expression("KEY")),
                        Schema.from(TEST_SCHEMA, Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of(
                                "value1",
                                Expressions.Expression("VALUE"),
                                "key1",
                                Expressions.Expression("KEY")),
                        Schema.from(TEST_SCHEMA, Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue")),
                arguments(
                        Map.of(
                                "timestamp",
                                Expressions.Expression("TIMESTAMP"),
                                "partition",
                                Expressions.Expression("PARTITION"),
                                "topic",
                                Expressions.Expression("TOPIC")),
                        Schema.from(TEST_SCHEMA, Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")));
    }

    @ParameterizedTest
    @MethodSource("stringExtractorArguments")
    public void shouldCreateAndExtractValues(
            String schemaName,
            Map<String, ExtractionExpression> expressions,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor = extractor(schemaName, expressions);

        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues container = extractor.extractData(kafkaRecord);

        Map<String, String> values = container.values();
        assertThat(values).isEqualTo(expectedValues);
    }
}
