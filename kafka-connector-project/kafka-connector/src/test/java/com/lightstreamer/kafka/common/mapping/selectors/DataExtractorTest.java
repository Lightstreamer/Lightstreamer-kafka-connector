
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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.emptyMap;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DataExtractorTest {

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

    static <K, V> DataExtractor<K, V> extractor(
            Map<String, ExtractionExpression> expressions,
            KeyValueSelectorSuppliers<K, V> suppliers)
            throws ExtractionException {
        return DataExtractor.<K, V>builder()
                .withSuppliers(suppliers)
                .withSchemaName(TEST_SCHEMA)
                .withExpressions(expressions)
                .build();
    }

    static Stream<Arguments> stringExtractorArguments() {
        return Stream.of(
                arguments(emptyMap(), Schema.empty(TEST_SCHEMA), emptyMap()),
                arguments(
                        Map.of("name", Expressions.expression("VALUE")),
                        Schema.from(TEST_SCHEMA, Set.of("name")),
                        Map.of("name", "aValue")),
                arguments(
                        Map.of(
                                "value",
                                Expressions.expression("VALUE"),
                                "key",
                                Expressions.expression("KEY")),
                        Schema.from(TEST_SCHEMA, Set.of("value", "key")),
                        Map.of("key", "aKey", "value", "aValue")),
                arguments(
                        Map.of(
                                "value1",
                                Expressions.expression("VALUE"),
                                "key1",
                                Expressions.expression("KEY")),
                        Schema.from(TEST_SCHEMA, Set.of("value1", "key1")),
                        Map.of("key1", "aKey", "value1", "aValue")),
                arguments(
                        Map.of(
                                "timestamp",
                                Expressions.expression("TIMESTAMP"),
                                "partition",
                                Expressions.expression("PARTITION"),
                                "topic",
                                Expressions.expression("TOPIC")),
                        Schema.from(TEST_SCHEMA, Set.of("timestamp", "partition", "topic")),
                        Map.of("partition", "150", "topic", "record-topic", "timestamp", "-1")));
    }

    @ParameterizedTest
    @MethodSource("stringExtractorArguments")
    public void shouldCreateAndExtractValues(
            Map<String, ExtractionExpression> expressions,
            Schema expectedSchema,
            Map<String, String> expectedValues)
            throws ExtractionException {
        DataExtractor<String, String> extractor =
                extractor(expressions, TestSelectorSuppliers.string());

        assertThat(extractor.schema()).isEqualTo(expectedSchema);

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("aKey", "aValue");
        SchemaAndValues container = extractor.extractData(kafkaRecord);

        Map<String, String> values = container.values();
        assertThat(values).isEqualTo(expectedValues);
    }
}
