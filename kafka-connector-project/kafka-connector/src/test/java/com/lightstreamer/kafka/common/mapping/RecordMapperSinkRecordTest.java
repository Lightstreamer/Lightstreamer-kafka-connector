
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

import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.SchemaAndValueProvider;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RecordMapperSinkRecordTest {

    private static DataExtractor<Object, Object> extractor(
            String schemaName, Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        return DataExtractor.builder()
                .withSuppliers(TestSelectorSuppliers.object())
                .withSchemaName(schemaName)
                .withExpressions(expressions)
                .build();
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
    public void shouldBuildMapperWithDuplicateSelectors() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("aKey", Expressions.expression("PARTITION"))))
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("aKey", Expressions.expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Test
    public void shouldBuildMapperWithDifferentSelectors() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test1",
                                        Map.of("aKey", Expressions.expression("PARTITION"))))
                        .withExtractor(
                                extractor(
                                        "test2",
                                        Map.of("aKey", Expressions.expression("PARTITION"))))
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
    public void shouldMapWithValues() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test1",
                                        Map.of("aKey", Expressions.expression("PARTITION"))))
                        .withExtractor(
                                extractor("test2", Map.of("aKey", Expressions.expression("TOPIC"))))
                        .withExtractor(
                                extractor(
                                        "test3",
                                        Map.of("aKey", Expressions.expression("TIMESTAMP"))))
                        .build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkRecord("topic", null, SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Test
    public void shoulNotFilterDueToUnboundSelectors() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                builder()
                        .withExtractor(
                                extractor(
                                        "test",
                                        Map.of("name", Expressions.expression("PARTITION"))))
                        .build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkRecord("topic", null, SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        DataExtractor<Object, Object> unboundExtractor =
                extractor("test", Map.of("name", Expressions.expression("VALUE.any")));
        assertThat(mappedRecord.filter(unboundExtractor)).isEmpty();
    }

    @Test
    public void shouldFilter() throws ExtractionException {
        DataExtractor<Object, Object> nameExtractor =
                extractor("test", Map.of("name", Expressions.expression("VALUE.name")));

        DataExtractor<Object, Object> childExtractor1 =
                extractor(
                        "test",
                        Map.of("firstChildName", Expressions.expression("VALUE.children[0].name")));

        DataExtractor<Object, Object> childExtractor2 =
                extractor(
                        "test",
                        Map.of(
                                "secondChildName",
                                Expressions.expression("VALUE.children[1].name"),
                                "grandChildName",
                                Expressions.expression("VALUE.children[1].children[1].name")));

        RecordMapper<Object, Object> mapper =
                builder()
                        .withExtractor(nameExtractor)
                        .withExtractor(childExtractor1)
                        .withExtractor(childExtractor2)
                        .build();

        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkFromValue(
                        "topic",
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
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
        DataExtractor<Object, Object> extractor =
                extractor(
                        "test",
                        Map.of(
                                "name",
                                Expressions.expression("VALUE.children[0].name"),
                                "signature",
                                Expressions.expression("VALUE.children[0].signature")));

        RecordMapper<Object, Object> mapper = builder().withExtractor(extractor).build();

        Struct STRUCT = SchemaAndValueProvider.STRUCT;
        KafkaRecord<Object, Object> kafkaRecord =
                KafkaRecord.from(
                        new SinkRecord("topic", 1, null, null, STRUCT.schema(), STRUCT, 0));
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(2);

        Map<String, String> parentName = mappedRecord.filter(extractor);
        assertThat(parentName).containsExactly("name", "alex", "signature", null);
    }
}
