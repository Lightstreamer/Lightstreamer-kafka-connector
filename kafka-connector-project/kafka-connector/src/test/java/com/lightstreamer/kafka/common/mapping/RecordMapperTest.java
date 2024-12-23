
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
import static com.lightstreamer.kafka.common.expressions.Expressions.Template;
import static com.lightstreamer.kafka.common.expressions.Expressions.Wrapped;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.AvroValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Object;

import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;
import com.lightstreamer.kafka.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka.test_utils.Records;
import com.lightstreamer.kafka.test_utils.SchemaAndValueProvider;

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
        assertThat(mapper.isRegexEnabled()).isFalse();
    }

    @Test
    public void shouldBuildMapperWithDuplicateTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), Template("test-#{aKey=PARTITION}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), Template("test-#{aKey=PARTITION}")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1))
                .containsExactly(
                        extractor(String(), Template("test-#{aKey=PARTITION}")));
    }

    @Test
    public void shouldBuildMapperWithDifferentTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1, extractor(String(), Template("prefix1-#{aKey=KEY}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), Template("prefix2-#{aValue=PARTITION}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        String(), Template("anotherPrefix-#{aKey=PARTITION}")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1)).hasSize(2);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1))
                .containsExactly(
                        extractor(String(), Template("prefix1-#{aKey=KEY}")),
                        extractor(String(), Template("prefix2-#{aValue=PARTITION}")));
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2))
                .containsExactly(
                        extractor(
                                String(),
                                Template("anotherPrefix-#{aKey=PARTITION}")));
    }

    @Test
    public void shouldBuildMapperWithTemplateExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractors(
                                Map.of(
                                        TEST_TOPIC_1,
                                        Set.of(
                                                extractor(
                                                        String(), Template("prefix1-#{aKey=KEY}")),
                                                extractor(
                                                        String(), Template("prefix2-#{aValue=PARTITION}"))),
                                        TEST_TOPIC_2,
                                        Set.of(
                                                extractor(
                                                        String(), Template("anotherPrefix-#{aKey=PARTITION}")))))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1)).hasSize(2);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1))
                .containsExactly(
                        extractor(String(), Template("prefix1-#{aKey=KEY}")),
                        extractor(String(), Template("prefix2-#{aValue=PARTITION}")));
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2))
                .containsExactly(
                        extractor(
                                String(),
                                Template("anotherPrefix-#{aKey=PARTITION}")));
    }

    @Test
    public void shouldBuildWithFieldsExtractor() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withFieldExtractor(
                                extractor(String(), Template("fields-#{aKey=PARTITION}")))
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
                                        Template("prefix1-#{partition=PARTITION,value=VALUE}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(String(), Template("prefix2-#{topic=TOPIC}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1, extractor(String(), Template("prefix3-#{key=KEY}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(String(), Template("prefix3-#{value=VALUE}")))
                        .withFieldExtractor(
                                extractor(
                                        String(),
                                        Template("fields-#{keyField=KEY,valueField=VALUE}")))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<String, String> kafkaRecord1 = Records.record(TEST_TOPIC_1, "aKey", "aValue");
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
                Records.record(TEST_TOPIC_2, "anotherKey", "anotherValue");
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromAnotherTopic = mappedRecord2.expanded();
        assertThat(expandedFromAnotherTopic)
                .containsExactly(SchemaAndValues.from("prefix3", Map.of("value", "anotherValue")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("keyField", "anotherKey", "valueField", "anotherValue");

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, String> kafkaRecord3 =
                Records.record("undefinedTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapRecordWithMatchingPattern() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withTemplateExtractor(
                                "topic[0-9]+",
                                extractor(
                                        String(), Template("prefix1-#{partition=PARTITION,value=VALUE}")))
                        .withTemplateExtractor(
                                "topic[0-9]+",
                                extractor(
                                        String(), Template("prefix2-#{topic=TOPIC}")))
                        .withTemplateExtractor(
                                "topic[0-9]+",
                                extractor(String(), Template("prefix3-#{key=KEY}")))
                        .withTemplateExtractor(
                                "anotherTopic[A-C]",
                                extractor(
                                        String(), Template("prefix3-#{value=VALUE}")))
                        .enableRegex(true)
                        .withFieldExtractor(
                                extractor(
                                        String(), Template("fields-#{keyField=KEY,valueField=VALUE}")))
                                        
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isTrue();

        // Record published to topic "topic0": mapping
        KafkaRecord<String, String> kafkaRecord1 = Records.record("topic0", "aKey", "aValue");
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        Set<SchemaAndValues> expandedFromTestsTopic = mappedRecord1.expanded();
        assertThat(expandedFromTestsTopic)
                .containsExactly(
                        SchemaAndValues.from(
                                "prefix1", Map.of("partition", "150", "value", "aValue")),
                        SchemaAndValues.from("prefix2", Map.of("topic", "topic0")),
                        SchemaAndValues.from("prefix3", Map.of("key", "aKey")));
        assertThat(mappedRecord1.fieldsMap())
                .containsExactly("keyField", "aKey", "valueField", "aValue");

        // Record published to topic "topic1": mapping
        KafkaRecord<String, String> kafkaRecord2 = Records.record("topic1", "aKey2", "aValue2");
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        Set<SchemaAndValues> expandedFromTopic1 = mappedRecord2.expanded();
        assertThat(expandedFromTopic1)
                .containsExactly(
                        SchemaAndValues.from(
                                "prefix1", Map.of("partition", "150", "value", "aValue2")),
                        SchemaAndValues.from("prefix2", Map.of("topic", "topic1")),
                        SchemaAndValues.from("prefix3", Map.of("key", "aKey2")));
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("keyField", "aKey2", "valueField", "aValue2");

        // Record published to topic "anotherTopicA": mapping
        KafkaRecord<String, String> kafkaRecord3 =
                Records.record("anotherTopicA", "anotherKey", "anotherValue");
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        Set<SchemaAndValues> expandedFromAntoherTopicA = mappedRecord3.expanded();
        assertThat(expandedFromAntoherTopicA)
                .containsExactly(SchemaAndValues.from("prefix3", Map.of("value", "anotherValue")));
        assertThat(mappedRecord3.fieldsMap())
                .containsExactly("keyField", "anotherKey", "valueField", "anotherValue");

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, String> kafkaRecord4 =
                Records.record("undefinedTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord4 = mapper.map(kafkaRecord4);
        assertThat(mappedRecord4.expanded()).isEmpty();
        assertThat(mappedRecord4.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapJsonRecordWithMatchingTopic() throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(JsonValue(), Template("test-#{name=VALUE.name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        JsonValue(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        JsonValue(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        JsonValue(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(
                                extractor(
                                        JsonValue(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                Wrapped("#{VALUE.children[0].signature}")),
                                        true))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<String, JsonNode> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", JsonNodeProvider.RECORD);
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
                Records.record(TEST_TOPIC_2, "", JsonNodeProvider.RECORD);
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
                Records.record("undefinedTopic", "", JsonNodeProvider.RECORD);
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
                                extractor(AvroValue(), Template("test-#{name=VALUE.name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        AvroValue(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        AvroValue(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        AvroValue(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(
                                extractor(
                                        AvroValue(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                Wrapped("#{VALUE.children[0].signature}")),
                                        true))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<String, GenericRecord> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", GenericRecordProvider.RECORD);
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
                Records.record(TEST_TOPIC_2, "", GenericRecordProvider.RECORD);
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
                Records.record("undefinedTopic", "", GenericRecordProvider.RECORD);
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
                                extractor(Object(), Template("test-#{name=VALUE.name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        Object(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_1,
                                extractor(
                                        Object(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .withTemplateExtractor(
                                TEST_TOPIC_2,
                                extractor(
                                        Object(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(
                                extractor(
                                        Object(),
                                        "fields",
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                Wrapped("#{VALUE.children[0].signature}")),
                                        false))
                        .build();
        assertThat(mapper.hasExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<Object, Object> kafkaRecord =
                Records.sinkFromValue(
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
                Records.sinkFromValue(
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
                Records.record(
                        "undefinedTopic",
                        SchemaAndValueProvider.STRUCT.schema(),
                        SchemaAndValueProvider.STRUCT);
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.expanded()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }
}
