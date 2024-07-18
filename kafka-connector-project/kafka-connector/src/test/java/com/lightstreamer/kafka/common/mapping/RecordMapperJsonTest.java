
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.expressions.ExpressionEvaluators.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperJsonTest {

    private static ValuesExtractor<String, JsonNode> extractor(
            String schemaName, Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        return ValuesExtractor.<String, JsonNode>builder()
                .withSuppliers(TestSelectorSuppliers.jsonValue(ConnectorConfigProvider.minimal()))
                .withSchemaName(schemaName)
                .withExpressions(expressions)
                .build();
    }

    private static Builder<String, JsonNode> builder() {
        return RecordMapper.<String, JsonNode>builder();
    }

    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<String, JsonNode> mapper = RecordMapper.<String, JsonNode>builder().build();
        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(0);
    }

    @Test
    public void shouldBuildMapperWithDuplicateSelectors() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("aKey", ExtractionExpression.of("PARTITION"))))
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("aKey", ExtractionExpression.of("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test1",
                                        Map.of("aKey", ExtractionExpression.of("PARTITION"))))
                        .withExtractor(
                                extractor(
                                        "test2",
                                        Map.of("aKey", ExtractionExpression.of("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, JsonNode> mapper = builder().build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test1",
                                        Map.of("aKey", ExtractionExpression.of("PARTITION"))))
                        .withExtractor(
                                extractor(
                                        "test2", Map.of("aKey", ExtractionExpression.of("TOPIC"))))
                        .withExtractor(
                                extractor(
                                        "test3",
                                        Map.of("aKey", ExtractionExpression.of("TIMESTAMP"))))
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("name", ExtractionExpression.of("PARTITION"))))
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        ValuesExtractor<String, JsonNode> unboundSelectors =
                extractor("test", Map.of("name", ExtractionExpression.of("VALUE.any")));
        assertThat(mappedRecord.filter(unboundSelectors)).isEmpty();
    }

    @Test
    public void shouldFilter() throws ExtractionException {
        ValuesExtractor<String, JsonNode> nameExtractor =
                extractor("test", Map.of("name", ExtractionExpression.of("VALUE.name")));

        ValuesExtractor<String, JsonNode> childExtractor1 =
                extractor(
                        "test",
                        Map.of(
                                "firstChildName",
                                ExtractionExpression.of("VALUE.children[0].name")));

        ValuesExtractor<String, JsonNode> childExtractor2 =
                extractor(
                        "test",
                        Map.of(
                                "secondChildName",
                                ExtractionExpression.of("VALUE.children[1].name"),
                                "grandChildName",
                                ExtractionExpression.of("VALUE.children[1].children[1].name")));

        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withExtractor(nameExtractor)
                        .withExtractor(childExtractor1)
                        .withExtractor(childExtractor2)
                        .build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(4);

        Map<String, String> parentName = mappedRecord.filter(nameExtractor);
        assertThat(parentName).containsExactly("name", "joe");

        Map<String, String> firstChildName = mappedRecord.filter(childExtractor1);
        assertThat(firstChildName).containsExactly("firstChildName", "alex");

        Map<String, String> otherPeopleNames = mappedRecord.filter(childExtractor2);
        assertThat(otherPeopleNames)
                .containsExactly("secondChildName", "anna", "grandChildName", "terence");
    }

    @Test
    public void shouldFilterWithNullValues() throws ExtractionException {
        ValuesExtractor<String, JsonNode> extractor =
                extractor(
                        "test",
                        Map.of(
                                "name",
                                ExtractionExpression.of("VALUE.children[0].name"),
                                "signature",
                                ExtractionExpression.of("VALUE.children[0].signature")));

        RecordMapper<String, JsonNode> mapper = builder().withExtractor(extractor).build();

        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(extractor);
        assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
