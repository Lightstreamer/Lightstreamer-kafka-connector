
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
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.with;

import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class RecordMapperTest {

    private static final String TEST_TOPIC = "topic";

    private static Builder<String, String> builder() {
        return RecordMapper.<String, String>builder();
    }

    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<String, String> mapper = builder().build();
        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isFalse();
        assertThat(mapper.hasFieldExtractor()).isFalse();
    }

    @Test
    public void shouldBuildMapperWithDuplicateTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(String(), "test", Map.of("aKey", Expression("PARTITION"))))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(String(), "test", Map.of("aKey", Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC))
                .containsExactly(with(String(), "test", Map.of("aKey", Expression("PARTITION"))));
    }

    @Test
    public void shouldBuildMapperWithDifferentTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(String(), "prefix1", Map.of("aKey", Expression("KEY"))))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(
                                        String(),
                                        "prefix2",
                                        Map.of("aValue", Expression("PARTITION"))))
                        .withTemplateExtractor(
                                "anotherTopic",
                                with(
                                        String(),
                                        "anotherPrefix",
                                        Map.of("aKey", Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();

        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC)).hasSize(2);
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC))
                .containsExactly(
                        with(String(), "prefix1", Map.of("aKey", Expression("KEY"))),
                        with(String(), "prefix2", Map.of("aValue", Expression("PARTITION"))));
        assertThat(mapper.getExtractorsByTopicName("anotherTopic")).hasSize(1);
        assertThat(mapper.getExtractorsByTopicName("anotherTopic"))
                .containsExactly(
                        with(String(), "anotherPrefix", Map.of("aKey", Expression("PARTITION"))));
    }

    @Test
    public void shouldBuildWithFieldsExtractor() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withFieldExtractor(
                                with(String(), "fields", Map.of("aKey", Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isFalse();
        assertThat(mapper.hasFieldExtractor()).isTrue();
    }

    @Test
    public void shouldMapRecordWithMatchingTopic() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(
                                        String(),
                                        "prefix1",
                                        Map.of(
                                                "partition",
                                                Expression("PARTITION"),
                                                "value",
                                                Expression("VALUE"))))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(String(), "prefix2", Map.of("topic", Expression("TOPIC"))))
                        .withTemplateExtractor(
                                TEST_TOPIC,
                                with(String(), "prefix3", Map.of("key", Expression("KEY"))))
                        .withTemplateExtractor(
                                "anotherTopic",
                                with(String(), "prefix3", Map.of("value", Expression("VALUE"))))
                        .withFieldExtractor(
                                with(
                                        String(),
                                        "fields",
                                        Map.of(
                                                "keyField",
                                                Expression("KEY"),
                                                "valueField",
                                                Expression("VALUE"))))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<String, String> kafkaRecord1 =
                ConsumerRecords.record(TEST_TOPIC, "aKey", "aValue");
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord1.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "prefix1", Map.of("partition", "150", "value", "aValue")),
                        SchemaAndValues.from("prefix2", Map.of("topic", TEST_TOPIC)),
                        SchemaAndValues.from("prefix3", Map.of("key", "aKey")));
        assertThat(mappedRecord1.fieldsMap())
                .containsExactly("keyField", "aKey", "valueField", "aValue");

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, String> kafkaRecord2 =
                ConsumerRecords.record("anotherTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromAnotherTopic = mappedRecord2.expanded();
        assertThat(expandedFromAnotherTopic)
                .containsExactly(SchemaAndValues.from("prefix3", Map.of("value", "anotherValue")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("keyField", "anotherKey", "valueField", "anotherValue");

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, String> kafkaRecord3 =
                ConsumerRecords.record("undefinedTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }
}
