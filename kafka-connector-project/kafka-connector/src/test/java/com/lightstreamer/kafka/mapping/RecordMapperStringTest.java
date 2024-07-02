
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
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperStringTest {

    private static ValuesExtractor<String, String> extractor(
            String schemaName, Map<String, String> expressions) {

        return ValuesExtractor.<String, String>builder()
                .withSuppliers(TestSelectorSuppliers.string())
                .withSchemaName(schemaName)
                .withExpressions(expressions)
                .build();
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
                        .withExtractor(extractor("test", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<String, String> mapper =
                builder()
                        .withExtractor(extractor("test1", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Test
    public void shouldMapEmpty() {
        RecordMapper<String, String> mapper = builder().build();

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Test
    public void shouldMapWithValues() {
        RecordMapper<String, String> mapper =
                builder()
                        .withExtractor(extractor("test1", Map.of("aKey", "PARTITION")))
                        .withExtractor(extractor("test2", Map.of("aKey", "TOPIC")))
                        .withExtractor(extractor("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record(null, "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<String, String> mapper =
                builder().withExtractor(extractor("test", Map.of("name", "PARTITION"))).build();

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        ValuesExtractor<String, String> unboundExtractor =
                extractor("test", Map.of("name", "VALUE"));
        assertThat(mappedRecord.filter(unboundExtractor)).isEmpty();
    }

    @Test
    public void shouldFilter() {
        ValuesExtractor<String, String> valueExtractor = extractor("test", Map.of("name", "VALUE"));
        ValuesExtractor<String, String> keyExtractor = extractor("test", Map.of("name", "KEY"));

        RecordMapper<String, String> mapper =
                builder().withExtractor(valueExtractor).withExtractor(keyExtractor).build();

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("", "aValue");
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(valueExtractor);
        assertThat(parentName).containsExactly("name", "aValue");

        Map<String, String> firstChildName = mappedRecord.filter(keyExtractor);
        assertThat(firstChildName).containsExactly("name", "");
    }

    @Test
    public void shouldFilterNulls() {
        ValuesExtractor<String, String> valueExtractor = extractor("test", Map.of("name", "VALUE"));
        ValuesExtractor<String, String> keyExtractor = extractor("test", Map.of("name", "KEY"));

        RecordMapper<String, String> mapper =
                builder().withExtractor(valueExtractor).withExtractor(keyExtractor).build();

        KafkaRecord<String, String> kafkaRecord = ConsumerRecords.record("", null);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(valueExtractor);
        assertThat(parentName).containsExactly("name", null);

        Map<String, String> firstChildName = mappedRecord.filter(keyExtractor);
        assertThat(firstChildName).containsExactly("name", "");
    }
}
