
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

import com.lightstreamer.kafka.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.Selectors;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.SchemaAndValueProvider;
import com.lightstreamer.kafka.test_utils.SelectedSuppplier;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperSinkRecordTest {

    private static Selectors<Object, Object> selectors(
            String schemaName, Map<String, String> entries) {
        return Selectors.from(SelectedSuppplier.object(), schemaName, entries);
    }

    private static Builder<Object, Object> builder() {
        return RecordMapper.<Object, Object>builder();
    }

    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<Object, Object> mapper = builder().build();
        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(0);
    }

    @Test
    public void shouldBuildMapperWithDuplicateSelectors() {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<Object, Object> mapper = builder().build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkRecord("topic", null, SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "TOPIC")))
                        .withSelectors(selectors("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkRecord("topic", null, SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<Object, Object> mapper =
                builder().withSelectors(selectors("test", Map.of("name", "PARTITION"))).build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkRecord("topic", null, SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        Selectors<Object, Object> unboundSelectors = selectors("test", Map.of("name", "VALUE.any"));
        assertThat(mappedRecord.filter(unboundSelectors)).isEmpty();
    }

    @Test
    public void shouldFilter() {
        Selectors<Object, Object> nameSelectors = selectors("test", Map.of("name", "VALUE.name"));

        Selectors<Object, Object> childSelectors1 =
                selectors("test", Map.of("firstChildName", "VALUE.children[0].name"));

        Selectors<Object, Object> childSelectors2 =
                selectors(
                        "test",
                        Map.of(
                                "secondChildName",
                                "VALUE.children[1].name",
                                "grandChildName",
                                "VALUE.children[1].children[1].name"));

        RecordMapper<Object, Object> mapper =
                builder()
                        .withSelectors(nameSelectors)
                        .withSelectors(childSelectors1)
                        .withSelectors(childSelectors2)
                        .build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sink(
                        "topic",
                        null,
                        null,
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
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
        Selectors<Object, Object> selectors =
                selectors(
                        "test",
                        Map.of(
                                "name",
                                "VALUE.children[0].name",
                                "signature",
                                "VALUE.children[0].signature"));

        RecordMapper<Object, Object> mapper = builder().withSelectors(selectors).build();

        Struct STRUCT = SchemaAndValueProvider.STRUCT;
        KafkaRecord<Object, Object> kafkaRecord =
                KafkaRecord.from(
                        new SinkRecord("topic", 1, null, null, STRUCT.schema(), STRUCT, 0));
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(selectors);
        assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
