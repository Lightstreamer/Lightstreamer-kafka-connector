
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

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperAvroTest {

    private static DataExtractor<String, GenericRecord> extractor(
            String schemaName, Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        "src/test/resources",
                        Map.of(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH, "value.avsc"));

        return DataExtractor.<String, GenericRecord>builder()
                .withSuppliers(TestSelectorSuppliers.AvroValue(config))
                .withSchemaName(schemaName)
                .withExpressions(expressions)
                .build();
    }

    private static Builder<String, GenericRecord> builder() {
        return RecordMapper.<String, GenericRecord>builder();
    }

    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<String, GenericRecord> mapper = builder().build();
        assertThat(mapper).isNotNull();
        // assertThat(mapper.selectorsSizeByTopic()).isEqualTo(0);
    }

    @Test
    public void shouldBuildMapperWithDuplicateSelectors() throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test",
                                        Map.of("aKey", Expressions.Expression("PARTITION"))))
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test",
                                        Map.of("aKey", Expressions.Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        // assertThat(mapper.selectorsSizeByTopic()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test1",
                                        Map.of("aKey", Expressions.Expression("PARTITION"))))
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test2",
                                        Map.of("aKey", Expressions.Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        // assertThat(mapper.selectorsSizeByTopic()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, GenericRecord> mapper = builder().build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        // assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test1",
                                        Map.of("aKey", Expressions.Expression("PARTITION"))))
                        .withTemplateExtractor(
                                "topic",
                                extractor("test2", Map.of("aKey", Expressions.Expression("TOPIC"))))
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test3",
                                        Map.of("aKey", Expressions.Expression("TIMESTAMP"))))
                        .build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        // assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withTemplateExtractor(
                                "topic",
                                extractor(
                                        "test",
                                        Map.of("name", Expressions.Expression("PARTITION"))))
                        .build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        DataExtractor<String, GenericRecord> unboundExtractor =
                extractor("test", Map.of("name", Expressions.Expression("VALUE.any")));
        // assertThat(mappedRecord.filter(unboundExtractor)).isEmpty();
    }

    @Test
    public void shouldFilter() throws ExtractionException {
        DataExtractor<String, GenericRecord> nameExtractor =
                extractor("test", Map.of("name", Expressions.Expression("VALUE.name")));

        DataExtractor<String, GenericRecord> childExtractor1 =
                extractor(
                        "test",
                        Map.of("firstChildName", Expressions.Expression("VALUE.children[0].name")));

        DataExtractor<String, GenericRecord> childExtractor2 =
                extractor(
                        "test",
                        Map.of(
                                "secondChildName",
                                Expressions.Expression("VALUE.children[1].name"),
                                "grandChildName",
                                Expressions.Expression("VALUE.children[1].children[1].name")));

        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withTemplateExtractor("topic", nameExtractor)
                        .withTemplateExtractor("topic", childExtractor1)
                        .withTemplateExtractor("topic", childExtractor2)
                        .build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        // assertThat(mappedRecord.mappedValuesSize()).isEqualTo(4);

        // Map<String, String> parentName = mappedRecord.filter(nameExtractor);
        // assertThat(parentName).containsExactly("name", "joe");

        // Map<String, String> firstChildName = mappedRecord.filter(childExtractor1);
        // assertThat(firstChildName).containsExactly("firstChildName", "alex");

        // Map<String, String> otherPeopleNames = mappedRecord.filter(childExtractor2);
        // assertThat(otherPeopleNames)
        //         .containsExactly("secondChildName", "anna", "grandChildName", "terence");
    }

    @Test
    public void shouldFilterWithNullValues() throws ExtractionException {
        DataExtractor<String, GenericRecord> extractor =
                extractor(
                        "test",
                        Map.of(
                                "name",
                                Expressions.Expression("VALUE.children[0].name"),
                                "signature",
                                Expressions.Expression("VALUE.children[0].signature")));

        RecordMapper<String, GenericRecord> mapper =
                builder().withTemplateExtractor("topic", extractor).build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        // assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        // Map<String, String> parentName = mappedRecord.filter(extractor);
        // assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
