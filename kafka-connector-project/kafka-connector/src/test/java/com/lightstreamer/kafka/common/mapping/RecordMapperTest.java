
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
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.canonicalItemExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.discoveredFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractors.namedFieldsExtractor;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleDynamicMessageProvider;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleGenericRecordProvider;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleStructProvider;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.AvroValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Object;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.ProtoValue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.common.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
        assertThat(mapper.hasCanonicalItemExtractors()).isFalse();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
    }

    @Test
    public void shouldBuildMapperWithDuplicateCanonicalItemExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(),
                                        Template("test-#{aPartition=PARTITION,anOffset=OFFSET}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(),
                                        Template("test-#{anOffset=OFFSET,aPartition=PARTITION}")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1))
                .containsExactly(
                        canonicalItemExtractor(
                                String(),
                                Template("test-#{aPartition=PARTITION,anOffset=OFFSET}")));
    }

    @Test
    public void shouldBuildMapperWithDifferentCanonicalItemExtractors() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(String(), Template("prefix1-#{aKey=KEY}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(), Template("prefix1-#{aKey=PARTITION}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(), Template("prefix2-#{aKey=PARTITION}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        String(), Template("anotherPrefix-#{aKey=PARTITION}")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isFalse();
        assertThat(mapper.isRegexEnabled()).isFalse();
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1)).hasSize(3);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_1))
                .containsExactly(
                        canonicalItemExtractor(String(), Template("prefix1-#{aKey=KEY}")),
                        canonicalItemExtractor(String(), Template("prefix1-#{aKey=PARTITION}")),
                        canonicalItemExtractor(String(), Template("prefix2-#{aKey=PARTITION}")));
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2)).hasSize(1);
        assertThat(mapper.getExtractorsByTopicSubscription(TEST_TOPIC_2))
                .containsExactly(
                        canonicalItemExtractor(
                                String(), Template("anotherPrefix-#{aKey=PARTITION}")));
    }

    @Test
    public void shouldBuildMapperWithStaticFieldsExtractor() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        String(),
                                        Map.of("aKey", Wrapped("#{PARTITION}")),
                                        false,
                                        false))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.hasCanonicalItemExtractors()).isFalse();
        assertThat(mapper.hasFieldExtractor()).isTrue();
    }

    @Test
    public void shouldMapRecordWithMatchingTopic() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(),
                                        Template("prefix1-#{partition=PARTITION,value=VALUE}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        String(), Template("prefix2-#{topic=TOPIC}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(String(), Template("prefix3-#{key=KEY}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        String(), Template("prefix3-#{value=VALUE}")))
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        String(),
                                        Map.of(
                                                "keyField",
                                                Wrapped("#{KEY}"),
                                                "valueField",
                                                Wrapped("#{VALUE}"),
                                                "headerValue",
                                                Wrapped("#{HEADERS.header-key1}")),
                                        false,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();

        // Record published to topic "topic": mapping
        KafkaRecord<String, String> kafkaRecord1 =
                Records.recordWithHeaders(
                        TEST_TOPIC_1,
                        "aKey",
                        "aValue",
                        new RecordHeaders().add("header-key1", "header-value1".getBytes()));
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord1.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "prefix1-[partition=150,value=aValue]",
                        "prefix2-[topic=topic]",
                        "prefix3-[key=aKey]");
        assertThat(mappedRecord1.fieldsMap())
                .containsExactly(
                        "keyField", "aKey", "valueField", "aValue", "headerValue", "header-value1");

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, String> kafkaRecord2 =
                Records.recordWithHeaders(
                        TEST_TOPIC_2,
                        "anotherKey",
                        "anotherValue",
                        new RecordHeaders().add("header-key1", "header-value1".getBytes()));
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] canonicalItemNamesFromAnotherTopic = mappedRecord2.canonicalItemNames();
        assertThat(canonicalItemNamesFromAnotherTopic)
                .asList()
                .containsExactly("prefix3-[value=anotherValue]");
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly(
                        "keyField",
                        "anotherKey",
                        "valueField",
                        "anotherValue",
                        "headerValue",
                        "header-value1");

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, String> kafkaRecord3 =
                Records.record("undefinedTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapRecordWithMatchingTopicPattern() throws ExtractionException {
        RecordMapper<String, String> mapper =
                builder()
                        .addCanonicalItemExtractor(
                                "topic[0-9]+",
                                canonicalItemExtractor(
                                        String(),
                                        Template("prefix1-#{partition=PARTITION,value=VALUE}")))
                        .addCanonicalItemExtractor(
                                "topic[0-9]+",
                                canonicalItemExtractor(
                                        String(), Template("prefix2-#{topic=TOPIC}")))
                        .addCanonicalItemExtractor(
                                "topic[0-9]+",
                                canonicalItemExtractor(String(), Template("prefix3-#{key=KEY}")))
                        .addCanonicalItemExtractor(
                                "anotherTopic[A-C]",
                                canonicalItemExtractor(
                                        String(), Template("prefix3-#{value=VALUE}")))
                        .enableRegex(true)
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        String(),
                                        Map.of(
                                                "keyField",
                                                Wrapped("#{KEY}"),
                                                "valueField",
                                                Wrapped("#{VALUE}")),
                                        false,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isTrue();

        // Record published to topic "topic0": mapping
        KafkaRecord<String, String> kafkaRecord1 = Records.record("topic0", "aKey", "aValue");
        MappedRecord mappedRecord1 = mapper.map(kafkaRecord1);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord1.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "prefix1-[partition=150,value=aValue]",
                        "prefix2-[topic=topic0]",
                        "prefix3-[key=aKey]");
        assertThat(mappedRecord1.fieldsMap())
                .containsExactly("keyField", "aKey", "valueField", "aValue");

        // Record published to topic "topic1": mapping
        KafkaRecord<String, String> kafkaRecord2 = Records.record("topic1", "aKey2", "aValue2");
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] canonicalItemNamesFromTopic1 = mappedRecord2.canonicalItemNames();
        assertThat(canonicalItemNamesFromTopic1)
                .asList()
                .containsExactly(
                        "prefix1-[partition=150,value=aValue2]",
                        "prefix2-[topic=topic1]",
                        "prefix3-[key=aKey2]");
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("keyField", "aKey2", "valueField", "aValue2");

        // Record published to topic "anotherTopicA": mapping
        KafkaRecord<String, String> kafkaRecord3 =
                Records.record("anotherTopicA", "anotherKey", "anotherValue");
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        String[] canonicalItemNamesFromAnotherTopicA = mappedRecord3.canonicalItemNames();
        assertThat(canonicalItemNamesFromAnotherTopicA)
                .asList()
                .containsExactly("prefix3-[value=anotherValue]");
        assertThat(mappedRecord3.fieldsMap())
                .containsExactly("keyField", "anotherKey", "valueField", "anotherValue");

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, String> kafkaRecord4 =
                Records.record("undefinedTopic", "anotherKey", "anotherValue");
        MappedRecord mappedRecord4 = mapper.map(kafkaRecord4);
        assertThat(mappedRecord4.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord4.fieldsMap()).isEmpty();
    }

    static Stream<Arguments> jsonFieldExtractors() throws ExtractionException {
        return Stream.of(
                arguments(
                        namedFieldsExtractor(
                                JsonValue(),
                                Map.of(
                                        "firstName",
                                        Wrapped("#{VALUE.name}"),
                                        "childSignature",
                                        Wrapped("#{VALUE.children[0].signature}")),
                                false,
                                false),
                        true),
                arguments(
                        discoveredFieldsExtractor(
                                JsonValue(), List.of(Wrapped("#{VALUE.children[0]}")), false),
                        false));
    }

    @ParameterizedTest
    @MethodSource("jsonFieldExtractors")
    public void shouldMapJsonRecordWithMatchingTopic(
            FieldsExtractor<String, JsonNode> fieldsExtractor, boolean isStatic)
            throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        JsonValue(), Template("test-#{name=VALUE.name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        JsonValue(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        JsonValue(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        JsonValue(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(fieldsExtractor)
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<String, JsonNode> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", SampleJsonNodeProvider().sampleMessage());
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "test-[name=joe]",
                        "test-[firstChildName=alex]",
                        "test-[grandChildName=terence,secondChildName=anna]");

        Map<String, String> fieldsMap = mappedRecord.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap).containsExactly("firstName", "joe", "childSignature", null);
        } else {
            assertThat(fieldsMap).containsAtLeast("name", "alex");
        }

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, JsonNode> kafkaRecord2 =
                Records.record(TEST_TOPIC_2, "", SampleJsonNodeProvider().sampleMessage());
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] canonicalItemNamesFromAnotherTopic = mappedRecord2.canonicalItemNames();
        assertThat(canonicalItemNamesFromAnotherTopic)
                .asList()
                .containsExactly("test-[grandChildName=gloria,thirdChildName=serena]");
        Map<String, String> fieldsMap2 = mappedRecord.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap2).containsExactly("firstName", "joe", "childSignature", null);
        } else {
            assertThat(fieldsMap2).containsAtLeast("name", "alex");
        }

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, JsonNode> kafkaRecord3 =
                Records.record("undefinedTopic", "", SampleJsonNodeProvider().sampleMessage());
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldSkipFieldMappingFailure() throws ExtractionException {
        // This flag will let field mapping alway success by omitting not mapped fields
        boolean skipOnFailure = true;
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        JsonValue(), Template("test-#{name=VALUE.name}")))
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        JsonValue(),
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                // This leads a ValueException, which will be
                                                // omitted
                                                Wrapped("#{VALUE.not_valid_attrib}")),
                                        skipOnFailure,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        KafkaRecord<String, JsonNode> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", SampleJsonNodeProvider().sampleMessage());
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        // The childSignature field has been skipped
        assertThat(mappedRecord.fieldsMap()).containsExactly("firstName", "joe");
    }

    @Test
    public void shouldNotSkipFieldMappingFailure() throws ExtractionException {
        boolean skipOnFailure = false;
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        JsonValue(), Template("test-#{name=VALUE.name}")))
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        JsonValue(),
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                // This leads a ValueException, which leads to make
                                                // getting the fieldsMap fail
                                                Wrapped("#{VALUE.not_valid_attrib}")),
                                        skipOnFailure,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        KafkaRecord<String, JsonNode> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", SampleJsonNodeProvider().sampleMessage());
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        ValueException ve = assertThrows(ValueException.class, () -> mappedRecord.fieldsMap());
        assertThat(ve).hasMessageThat().isEqualTo("Field [not_valid_attrib] not found");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotMapDueToTemplateFailure(boolean skipOnFailure) throws ExtractionException {
        RecordMapper<String, JsonNode> mapper =
                RecordMapper.<String, JsonNode>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                // This leads a ValueException, which leads to make mapping fail
                                canonicalItemExtractor(
                                        JsonValue(),
                                        Template("test-#{name=VALUE.not_valid_attrib}")))
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        JsonValue(),
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                Wrapped("#{VALUE.children[0].signature}")),
                                        // This flag is irrelevant when failure happens in template
                                        // extractor
                                        skipOnFailure,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                mapper.map(
                                        Records.record(
                                                TEST_TOPIC_1,
                                                "",
                                                SampleJsonNodeProvider().sampleMessage())));
        assertThat(ve).hasMessageThat().isEqualTo("Field [not_valid_attrib] not found");
    }

    static Stream<Arguments> avoFieldExtractors() throws ExtractionException {
        return Stream.of(
                arguments(
                        namedFieldsExtractor(
                                AvroValue(),
                                Map.of(
                                        "firstName",
                                        Wrapped("#{VALUE.name}"),
                                        "childSignature",
                                        Wrapped("#{VALUE.children[0].signature}")),
                                false,
                                false),
                        true),
                arguments(
                        discoveredFieldsExtractor(
                                AvroValue(), List.of(Wrapped("#{VALUE.children[0]}")), false),
                        false));
    }

    @ParameterizedTest
    @MethodSource("avoFieldExtractors")
    public void shouldMapAvroRecordWithMatchingTopic(
            FieldsExtractor<String, GenericRecord> fieldExtractor, boolean isStatic)
            throws ExtractionException {
        RecordMapper<String, GenericRecord> mapper =
                RecordMapper.<String, GenericRecord>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        AvroValue(), Template("test-#{name=VALUE.name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        AvroValue(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        AvroValue(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        AvroValue(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(fieldExtractor)
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<String, GenericRecord> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", SampleGenericRecordProvider().sampleMessage());
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "test-[name=joe]",
                        "test-[firstChildName=alex]",
                        "test-[grandChildName=terence,secondChildName=anna]");

        Map<String, String> fieldsMap = mappedRecord.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap).containsExactly("firstName", "joe", "childSignature", null);
        } else {
            assertThat(fieldsMap).containsAtLeast("name", "alex");
        }

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, GenericRecord> kafkaRecord2 =
                Records.record(TEST_TOPIC_2, "", SampleGenericRecordProvider().sampleMessage());
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] canonicalItemNamesFromAnotherTopic = mappedRecord2.canonicalItemNames();
        assertThat(canonicalItemNamesFromAnotherTopic)
                .asList()
                .containsExactly("test-[grandChildName=gloria,thirdChildName=serena]");
        Map<String, String> fieldsMap2 = mappedRecord2.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap2).containsExactly("firstName", "joe", "childSignature", null);
        } else {
            assertThat(fieldsMap2).containsAtLeast("name", "alex");
        }

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, GenericRecord> kafkaRecord3 =
                Records.record("undefinedTopic", "", SampleGenericRecordProvider().sampleMessage());
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    static Stream<Arguments> protobufFieldExtractors() throws ExtractionException {
        return Stream.of(
                arguments(
                        namedFieldsExtractor(
                                ProtoValue(),
                                Map.of(
                                        "firstName",
                                        Wrapped("#{VALUE.name}"),
                                        "friendSignature",
                                        Wrapped("#{VALUE.friends[1].friends[0].signature}")),
                                false,
                                false),
                        true),
                arguments(
                        discoveredFieldsExtractor(
                                ProtoValue(),
                                List.of(Wrapped("#{VALUE.friends[1].friends[0]}")),
                                false),
                        false));
    }

    @ParameterizedTest
    @MethodSource("protobufFieldExtractors")
    public void shouldMapProtobufRecordWithMatchingTopic(
            FieldsExtractor<String, DynamicMessage> fieldsExtractor, boolean isStatic)
            throws ExtractionException {
        RecordMapper<String, DynamicMessage> mapper =
                RecordMapper.<String, DynamicMessage>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        ProtoValue(), Template("test-#{name=VALUE.name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        ProtoValue(),
                                        Template("test-#{firstFriendName=VALUE.friends[0].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        ProtoValue(),
                                        Template(
                                                "test-#{secondFriendName=VALUE.friends[1].name,otherName=VALUE.friends[1].friends[0].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        ProtoValue(),
                                        Template(
                                                "test-#{phoneNumber=VALUE.phoneNumbers[0],country=VALUE.otherAddresses['work'].country.name}")))
                        .withFieldExtractor(fieldsExtractor)
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        KafkaRecord<String, DynamicMessage> kafkaRecord =
                Records.record(TEST_TOPIC_1, "", SampleDynamicMessageProvider().sampleMessage());
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "test-[name=joe]",
                        "test-[firstFriendName=mike]",
                        "test-[otherName=robert,secondFriendName=john]");
        Map<String, String> fieldsMap = mappedRecord.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap).containsExactly("firstName", "joe", "friendSignature", "abcd");
        } else {
            assertThat(fieldsMap).containsAtLeast("name", "robert", "signature", "abcd");
        }

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<String, DynamicMessage> kafkaRecord2 =
                Records.record(TEST_TOPIC_2, "", SampleDynamicMessageProvider().sampleMessage());
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] canonicalItemNamesFromAnotherTopic = mappedRecord2.canonicalItemNames();
        assertThat(canonicalItemNamesFromAnotherTopic)
                .asList()
                .containsExactly("test-[country=Italy,phoneNumber=012345]");
        Map<String, String> fieldsMap2 = mappedRecord2.fieldsMap();
        if (isStatic) {
            assertThat(fieldsMap2).containsExactly("firstName", "joe", "friendSignature", "abcd");
        } else {
            assertThat(fieldsMap2).containsAtLeast("name", "robert", "signature", "abcd");
        }

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<String, DynamicMessage> kafkaRecord3 =
                Records.record(
                        "undefinedTopic", "", SampleDynamicMessageProvider().sampleMessage());
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }

    @Test
    public void shouldMapSinkRecordMatchingTopic() throws ExtractionException {
        RecordMapper<Object, Object> mapper =
                RecordMapper.<Object, Object>builder()
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        Object(), Template("test-#{name=VALUE.name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        Object(),
                                        Template("test-#{firstChildName=VALUE.children[0].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_1,
                                canonicalItemExtractor(
                                        Object(),
                                        Template(
                                                "test-#{secondChildName=VALUE.children[1].name,grandChildName=VALUE.children[1].children[1].name}")))
                        .addCanonicalItemExtractor(
                                TEST_TOPIC_2,
                                canonicalItemExtractor(
                                        Object(),
                                        Template(
                                                "test-#{thirdChildName=VALUE.children[2].name,grandChildName=VALUE.children[1].children[0].name}")))
                        .withFieldExtractor(
                                namedFieldsExtractor(
                                        Object(),
                                        Map.of(
                                                "firstName",
                                                Wrapped("#{VALUE.name}"),
                                                "childSignature",
                                                Wrapped("#{VALUE.children[0].signature}")),
                                        false,
                                        false))
                        .build();
        assertThat(mapper.hasCanonicalItemExtractors()).isTrue();
        assertThat(mapper.hasFieldExtractor()).isTrue();
        assertThat(mapper.isRegexEnabled()).isFalse();

        // Record published to topic "topic": mapping
        Struct message = SampleStructProvider().sampleMessage();
        KafkaRecord<Object, Object> kafkaRecord =
                Records.sinkFromValue(TEST_TOPIC_1, message.schema(), message);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        String[] canonicalItemNamesFromTestsTopic = mappedRecord.canonicalItemNames();
        assertThat(canonicalItemNamesFromTestsTopic)
                .asList()
                .containsExactly(
                        "test-[name=joe]",
                        "test-[firstChildName=alex]",
                        "test-[grandChildName=terence,secondChildName=anna]");
        assertThat(mappedRecord.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "anotherTopic": mapping
        KafkaRecord<Object, Object> kafkaRecord2 =
                Records.sinkFromValue(TEST_TOPIC_2, message.schema(), message);
        MappedRecord mappedRecord2 = mapper.map(kafkaRecord2);
        String[] itemNamesFromAnotherTopic = mappedRecord2.canonicalItemNames();
        assertThat(itemNamesFromAnotherTopic)
                .asList()
                .containsExactly("test-[grandChildName=gloria,thirdChildName=serena]");
        assertThat(mappedRecord2.fieldsMap())
                .containsExactly("firstName", "joe", "childSignature", null);

        // Record published to topic "undefinedTopic": no mapping
        KafkaRecord<Object, Object> kafkaRecord3 =
                Records.record("undefinedTopic", message.schema(), message);
        MappedRecord mappedRecord3 = mapper.map(kafkaRecord3);
        assertThat(mappedRecord3.canonicalItemNames()).isEmpty();
        assertThat(mappedRecord3.fieldsMap()).isEmpty();
    }
}
