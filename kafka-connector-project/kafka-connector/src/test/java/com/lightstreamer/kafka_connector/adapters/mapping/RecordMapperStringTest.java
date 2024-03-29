
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

import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperStringTest {

    private static Selectors<String, String> selectors(
            String schemaName, Map<String, String> entries) {
        return Selectors.from(SelectorsSuppliers.string(), schemaName, entries);
    }

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<String, String> mapper = builder().build();
        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(0);
    }

    @Test
    public void shouldBuildMapperWithDuplicateSelectors() {
        RecordMapper<String, String> mapper =
                builder()
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<String, String> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, String> mapper = builder().build();

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() {
        RecordMapper<String, String> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "TOPIC")))
                        .withSelectors(selectors("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record(null, "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<String, String> mapper =
                builder().withSelectors(selectors("test", Map.of("name", "PARTITION"))).build();

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        Selectors<String, String> unboundSelectors = selectors("test", Map.of("name", "VALUE"));
        assertThat(mappedRecord.filter(unboundSelectors)).isEmpty();
    }

    @Test
    public void shouldFilter() {
        Selectors<String, String> valueSelectors = selectors("test", Map.of("name", "VALUE"));
        Selectors<String, String> keySelectors = selectors("test", Map.of("name", "KEY"));

        RecordMapper<String, String> mapper =
                builder().withSelectors(valueSelectors).withSelectors(keySelectors).build();

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(valueSelectors);
        assertThat(parentName).containsExactly("name", "aValue");

        Map<String, String> firstChildName = mappedRecord.filter(keySelectors);
        assertThat(firstChildName).containsExactly("name", "");
    }

    @Test
    public void shouldFilterNulls() {
        Selectors<String, String> valueSelectors = selectors("test", Map.of("name", "VALUE"));
        Selectors<String, String> keySelectors = selectors("test", Map.of("name", "KEY"));

        RecordMapper<String, String> mapper =
                builder().withSelectors(valueSelectors).withSelectors(keySelectors).build();

        ConsumerRecord<String, String> kafkaRecord = ConsumerRecords.record("", null);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(valueSelectors);
        assertThat(parentName).containsExactly("name", null);

        Map<String, String> firstChildName = mappedRecord.filter(keySelectors);
        assertThat(firstChildName).containsExactly("name", "");
    }
}
