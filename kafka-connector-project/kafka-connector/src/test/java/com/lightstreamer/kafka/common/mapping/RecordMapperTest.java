
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.AvroValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Object;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.SchemaAndValueProvider;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class RecordMapperTest {

    private static final String TEST_TOPIC_1 = "topic";
    private static final String TEST_TOPIC_2 = "anotherTopic";

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
                                TEST_TOPIC_1,
                                extractor(
                                        String(), "test", Map.of("aKey", Expression("PARTITION"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        String(), "test", Map.of("aKey", Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_1)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_1))
                .containsExactly(
                        extractor(String(), "test", Map.of("aKey", Expression("PARTITION"))));
    }

    @Test
    public void shouldBuildMapperWithDifferentTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), "prefix1", Map.of("aKey", Expression("KEY"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        String(),
                                        "prefix2",
                                        Map.of("aValue", Expression("PARTITION"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        String(),
                                        "anotherPrefix",
                                        Map.of("aKey", Expression("PARTITION"))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();

        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_1)).hasSize(2);
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_1))
                .containsExactly(
                        extractor(String(), "prefix1", Map.of("aKey", Expression("KEY"))),
                        extractor(String(), "prefix2", Map.of("aValue", Expression("PARTITION"))));
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_2)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicName(TEST_TOPIC_2))
                .containsExactly(
                        extractor(
                                String(),
                                "anotherPrefix",
                                Map.of("aKey", Expression("PARTITION"))));
    }

    @Test
    public void shouldBuildWithFieldsExtractor() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withFieldExtractor(
                                extractor(
                                        String(),
                                        "fields",
                                        Map.of("aKey", Expression("PARTITION"))))
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
                                TEST_TOPIC_1,
                                extractor(
                                        String(),
                                        "prefix1",
                                        Map.of(
                                                "partition",
                                                Expression("PARTITION"),
                                                "value",
                                                Expression("VALUE"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        String(), "prefix2", Map.of("topic", Expression("TOPIC"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), "prefix3", Map.of("key", Expression("KEY"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        String(), "prefix3", Map.of("value", Expression("VALUE"))))
                        .withFieldExtractor(
                                extractor(
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
                ConsumerRecords.record(TEST_TOPIC_1, "aKey", "aValue");
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord1.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "prefix1", Map.of("partition", "150", "value", "aValue")),
                        SchemaAndValues.from("prefix2", Map.of("topic", TEST_TOPIC_1)),
                        SchemaAndValues.from("prefix3", Map.of("key", "aKey")));
        assertThat(mappedRecord1.fieldsMap())
                .containsExactly("keyField", "aKey", "valueField", "aValue");

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, String> kafkaRecord2 =
                ConsumerRecords.record(TEST_TOPIC_2, "anotherKey", "anotherValue");
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

    @Test
    public void shouldMapJsonRecordWithMatchingTopic() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        JsonValue(),
                                        "test",
                                        Map.of("name", Expression("VALUE.name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        JsonValue(),
                                        "test",
                                        Map.of(
                                                "firstChildName",
                                                Expression("VALUE.children[0].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        JsonValue(),
                                        "test",
                                        Map.of(
                                                "secondChildName",
                                                Expression("VALUE.children[1].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[1].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        JsonValue(),
                                        "test",
                                        Map.of(
                                                "thirdChildName",
                                                Expression("VALUE.children[2].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[0].name"))))
                        .withFieldExtractor(
                                extractor(
                                        JsonValue(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Expression("VALUE.name"),
                                                "childSignature",
                                                Expression("VALUE.children[0].signature"))))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record(TEST_TOPIC_1, "", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from("test", Map.of("name", "joe")),
                        SchemaAndValues.from("test", Map.of("firstChildName", "alex")),
                        SchemaAndValues.from(
                                "test",
                                Map.of("secondChildName", "anna", "grandChildName", "terence")));
        assertThat(mappedRecord.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                ConsumerRecords.record(TEST_TOPIC_2, "", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromAnotherTopic = mappedRecord2.expanded();
        assertThat(expandedFromAnotherTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "test",
                                Map.of("thirdChildName", "serena", "grandChildName", "gloria")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, JsonNode> kafkaRecord3 =
                ConsumerRecords.record("undefinedTopic", "", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapAvroRecordWithMatchingTopic() throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                RecordMapper.<String, GenericRecord>builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        AvroValue(),
                                        "test",
                                        Map.of("name", Expression("VALUE.name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        AvroValue(),
                                        "test",
                                        Map.of(
                                                "firstChildName",
                                                Expression("VALUE.children[0].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        AvroValue(),
                                        "test",
                                        Map.of(
                                                "secondChildName",
                                                Expression("VALUE.children[1].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[1].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        AvroValue(),
                                        "test",
                                        Map.of(
                                                "thirdChildName",
                                                Expression("VALUE.children[2].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[0].name"))))
                        .withFieldExtractor(
                                extractor(
                                        AvroValue(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Expression("VALUE.name"),
                                                "childSignature",
                                                Expression("VALUE.children[0].signature"))))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<String, GenericRecord> kafkaRecord =
                ConsumerRecords.record(TEST_TOPIC_1, "", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from("test", Map.of("name", "joe")),
                        SchemaAndValues.from("test", Map.of("firstChildName", "alex")),
                        SchemaAndValues.from(
                                "test",
                                Map.of("secondChildName", "anna", "grandChildName", "terence")));
        assertThat(mappedRecord.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, GenericRecord> kafkaRecord2 =
                ConsumerRecords.record(TEST_TOPIC_2, "", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromAnotherTopic = mappedRecord2.expanded();
        assertThat(expandedFromAnotherTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "test",
                                Map.of("thirdChildName", "serena", "grandChildName", "gloria")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, GenericRecord> kafkaRecord3 =
                ConsumerRecords.record("undefinedTopic", "", GenericRecordProvider.RECORD);
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapSinkRecordMatchingTopic() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                RecordMapper.<Object, Object>builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        Object(), "test", Map.of("name", Expression("VALUE.name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        Object(),
                                        "test",
                                        Map.of(
                                                "firstChildName",
                                                Expression("VALUE.children[0].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        Object(),
                                        "test",
                                        Map.of(
                                                "secondChildName",
                                                Expression("VALUE.children[1].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[1].name"))))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        Object(),
                                        "test",
                                        Map.of(
                                                "thirdChildName",
                                                Expression("VALUE.children[2].name"),
                                                "grandChildName",
                                                Expression("VALUE.children[1].children[0].name"))))
                        .withFieldExtractor(
                                extractor(
                                        Object(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Expression("VALUE.name"),
                                                "childSignature",
                                                Expression("VALUE.children[0].signature"))))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<Object, Object> kafkaRecord =
                ConsumerRecords.sinkFromValue(
                        TEST_TOPIC_1,
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from("test", Map.of("name", "joe")),
                        SchemaAndValues.from("test", Map.of("firstChildName", "alex")),
                        SchemaAndValues.from(
                                "test",
                                Map.of("secondChildName", "anna", "grandChildName", "terence")));
        assertThat(mappedRecord.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<Object, Object> kafkaRecord2 =
                ConsumerRecords.sinkFromValue(
                        TEST_TOPIC_2,
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromAnotherTopic = mappedRecord2.expanded();
        assertThat(expandedFromAnotherTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "test",
                                Map.of("thirdChildName", "serena", "grandChildName", "gloria")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<Object, Object> kafkaRecord3 =
                ConsumerRecords.record(
                        "undefinedTopic",
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }
}
