
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

package com.lightstreamer.kafka_connector.adapters.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapters.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperAvroTest {

    private static Selectors<String, GenericRecord> selectors(
            String schemaName, Map<String, String> entries) {
        return Selectors.from(
                SelectorsSuppliers.avroValue(
                        ConnectorConfigProvider.minimalWith(
                                "src/test/resources",
                                Map.of(
                                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                        "value.avsc"))),
                schemaName,
                entries);
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
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, GenericRecord> mapper = builder().build();

        ConsumerRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() {
        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "TOPIC")))
                        .withSelectors(selectors("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        ConsumerRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<String, GenericRecord> mapper =
                builder().withSelectors(selectors("test", Map.of("name", "PARTITION"))).build();

        ConsumerRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        Selectors<String, GenericRecord> unboundSelectors =
                selectors("test", Map.of("name", "VALUE.any"));
        assertThat(mappedRecord.filter(unboundSelectors)).isEmpty();
    }

    @Test
    public void shouldFilter() {
        Selectors<String, GenericRecord> nameSelectors =
                selectors("test", Map.of("name", "VALUE.name"));

        Selectors<String, GenericRecord> childSelectors1 =
                selectors("test", Map.of("firstChildName", "VALUE.children[0].name"));

        Selectors<String, GenericRecord> childSelectors2 =
                selectors(
                        "test",
                        Map.of(
                                "secondChildName",
                                "VALUE.children[1].name",
                                "grandChildName",
                                "VALUE.children[1].children[1].name"));

        RecordMapper<String, GenericRecord> mapper =
                builder()
                        .withSelectors(nameSelectors)
                        .withSelectors(childSelectors1)
                        .withSelectors(childSelectors2)
                        .build();

        ConsumerRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(4);

        Map<String, String> parentName = mappedRecord.filter(nameSelectors);
        assertThat(parentName).containsExactly("name", "joe");

        Map<String, String> firstChildName = mappedRecord.filter(childSelectors1);
        assertThat(firstChildName).containsExactly("firstChildName", "alex");

        Map<String, String> otherPeopleNames = mappedRecord.filter(childSelectors2);
        assertThat(otherPeopleNames)
                .containsExactly("secondChildName", "anna", "grandChildName", "terence");
    }

    @Test
    public void shouldFilterWithNullValues() {
        Selectors<String, GenericRecord> selectors =
                selectors(
                        "test",
                        Map.of(
                                "name",
                                "VALUE.children[0].name",
                                "signature",
                                "VALUE.children[0].signature"));

        RecordMapper<String, GenericRecord> mapper = builder().withSelectors(selectors).build();

        ConsumerRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(selectors);
        assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
