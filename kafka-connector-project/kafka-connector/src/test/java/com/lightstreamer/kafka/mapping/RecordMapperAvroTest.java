
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

package com.lightstreamer.kafka.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperAvroTest {

    private static ValuesExtractor<String, GenericRecord> extractor(
            String schemaName, Map<String, String> expressions) {
        SelectorSuppliers<String, GenericRecord> avroValue =
                TestSelectorSuppliers.avroValue(
                        ConnectorConfigProvider.minimalWith(
                                "src/test/resources",
                                Map.of(
                                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                        "value.avsc")));
        return ValuesExtractor.<String, GenericRecord>builder()
                .withSuppliers(avroValue)
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
        assertThat(mapper.selectorsSize()).isEqualTo(0);
    }

    @Test
    public void shouldBuildMapperWithDuplicateSelectors() {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withExtractor(extractor("test", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withExtractor(extractor("test1", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, GenericRecord> mapper = builder().build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withExtractor(extractor("test1", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test2", Map.of("aKey", "TOPIC")))
                        .withExtractor(extractor("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<String, GenericRecord> mapper =
                builder().withExtractor(extractor("test", Map.of("name", "PARTITION"))).build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        ValuesExtractor<String, GenericRecord> unboundExtractor =
                extractor("test", Map.of("name", "VALUE.any"));
        assertThat(mappedRecord.filter(unboundExtractor)).isEmpty();
    }

    @Test
    public void shouldFilter() {
        ValuesExtractor<String, GenericRecord> nameExtractor =
                extractor("test", Map.of("name", "VALUE.name"));

        ValuesExtractor<String, GenericRecord> childExtractor1 =
                extractor("test", Map.of("firstChildName", "VALUE.children[0].name"));

        ValuesExtractor<String, GenericRecord> childExtractor2 =
                extractor(
                        "test",
                        Map.of(
                                "secondChildName",
                                "VALUE.children[1].name",
                                "grandChildName",
                                "VALUE.children[1].children[1].name"));

        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withExtractor(nameExtractor)
                        .withExtractor(childExtractor1)
                        .withExtractor(childExtractor2)
                        .build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
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
    public void shouldFilterWithNullValues() {
        ValuesExtractor<String, GenericRecord> extractor =
                extractor(
                        "test",
                        Map.of(
                                "name",
                                "VALUE.children[0].name",
                                "signature",
                                "VALUE.children[0].signature"));

        RecordMapper<String, GenericRecord> mapper = builder().withExtractor(extractor).build();

        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(extractor);
        assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
