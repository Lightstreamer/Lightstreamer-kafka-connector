
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

package com.lightstreamer.kafka.connect.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.sinkFromKey;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.sinkFromValue;
import static com.lightstreamer.kafka.test_utils.SchemaAndValueProvider.STRUCT;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.ValueException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

public class ConnectSelectorsSuppliersTest {

    private static final SchemaBuilder FLAT_SCHEMA_BUILDER =
            SchemaBuilder.struct()
                    .field("int8", Schema.INT8_SCHEMA)
                    .field("int16", Schema.INT16_SCHEMA)
                    .field("int32", Schema.INT32_SCHEMA)
                    .field("int64", Schema.INT64_SCHEMA)
                    .field("float32", Schema.FLOAT32_SCHEMA)
                    .field("float64", Schema.FLOAT64_SCHEMA)
                    .field("boolean", Schema.BOOLEAN_SCHEMA)
                    .field("string", Schema.STRING_SCHEMA)
                    .field("bytes", Schema.BYTES_SCHEMA)
                    .field("byteBuffer", Schema.BYTES_SCHEMA);

    private static final Schema OPTIONAL_FLAT_SCHEMA = FLAT_SCHEMA_BUILDER.optional().build();

    private static final Schema OPTIONAL_SIMPLE_MAP_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();

    private static final Schema OPTIONAL_COMPLEX_MAP_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, OPTIONAL_FLAT_SCHEMA).optional().build();

    private static final Schema OPTIONAL_MAP_OF_MAP_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, OPTIONAL_SIMPLE_MAP_SCHEMA).optional().build();

    private static final Schema NESTED_SCHEMA =
            SchemaBuilder.struct()
                    .field("nested", OPTIONAL_FLAT_SCHEMA)
                    .field("map", OPTIONAL_SIMPLE_MAP_SCHEMA)
                    .field("complexMap", OPTIONAL_COMPLEX_MAP_SCHEMA)
                    .field("mapOfMap", OPTIONAL_MAP_OF_MAP_SCHEMA)
                    .build();

    static ConnectValueSelector valueSelector(String expression) {
        return ConnectSelectorsSuppliers.valueSelectorSupplier(false)
                .newSelector("name", expression);
    }

    static ConnectKeySelector keySelector(String expression) {
        return ConnectSelectorsSuppliers.keySelectorSupplier(false).newSelector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required becase of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION                             |  EXPECTED
                        VALUE.name                             |  joe
                        VALUE.signature                        |  [97, 98, 99, 100]
                        VALUE.children[0].name                 |  alex
                        VALUE.children[0]['name']              |  alex
                        VALUE.children[0].signature            |  NULL
                        VALUE.children[1].name                 |  anna
                        VALUE.children[2].name                 |  serena
                        VALUE.children[3]                      |  NULL
                        VALUE.children[1].children[0].name     |  gloria
                        VALUE.children[1].children[1].name     |  terence
                        VALUE.children[1].children[1]['name']  |  terence
                        """)
    public void shouldExtractValue(String expression, String expected) {
        StringSubject subject =
                assertThat(
                        valueSelector(expression)
                                .extract(sinkFromValue("topic", STRUCT.schema(), STRUCT))
                                .text());
        if (expected.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                   EXPECTED_ERROR_MESSAGE
                        VALUE,                        The expression [VALUE] must evaluate to a non-complex object
                        VALUE.no_attrib,              Field [no_attrib] not found
                        VALUE.children[0].no_attrib,  Field [no_attrib] not found
                        VALUE.no_children[0],         Field [no_children] not found
                        VALUE.name[0],                Field [name] is not indexed
                        VALUE.name['no_key'],         Field [no_key] not found
                        VALUE.children,               The expression [VALUE.children] must evaluate to a non-complex object
                        VALUE.children[0]['no_key'],  Field [no_key] not found
                        VALUE.children[0],            The expression [VALUE.children[0]] must evaluate to a non-complex object
                        VALUE.children[3].name,       Cannot retrieve field [name] from a null object
                        VALUE.children[4],            Field not found at index [4]
                        VALUE.children[4].name,       Field not found at index [4]
                        """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extract(sinkFromValue("topic", STRUCT.schema(), STRUCT)));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required becase of the expected value for input KEY.signature
            textBlock =
                    """
                        EXPRESSION                           | EXPECTED
                        KEY.name                             | joe
                        KEY.signature                        | [97, 98, 99, 100]
                        KEY.children[0].name                 | alex
                        KEY.children[0]['name']              | alex
                        KEY.children[0].signature            | NULL
                        KEY.children[1].name                 | anna
                        KEY.children[2].name                 | serena
                        KEY.children[3]                      | NULL
                        KEY.children[1].children[0].name     | gloria
                        KEY.children[1].children[1].name     | terence
                        KEY.children[1].children[1]['name']  | terence
                        """)
    public void shouldExtractKey(String expression, String expected) {
        StringSubject subject =
                assertThat(
                        keySelector(expression)
                                .extract(sinkFromKey("topic", STRUCT.schema(), STRUCT))
                                // .extract(sinkFromKey("topic", null, STRUCT))
                                .text());
        if (expected.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                 EXPECTED_ERROR_MESSAGE
                        KEY,                        The expression [KEY] must evaluate to a non-complex object
                        KEY.no_attrib,              Field [no_attrib] not found
                        KEY.children[0].no_attrib,  Field [no_attrib] not found
                        KEY.no_children[0],         Field [no_children] not found
                        KEY.name[0],                Field [name] is not indexed
                        KEY.name['no_key'],         Field [no_key] not found
                        KEY.children,               The expression [KEY.children] must evaluate to a non-complex object
                        KEY.children[0]['no_key'],  Field [no_key] not found
                        KEY.children[0],            The expression [KEY.children[0]] must evaluate to a non-complex object
                        KEY.children[3].name,       Cannot retrieve field [name] from a null object
                        KEY.children[4],            Field not found at index [4]
                        KEY.children[4].name,       Field not found at index [4]
                        """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extract(sinkFromKey("topic", STRUCT.schema(), STRUCT)));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,          EXPECTED_ERROR_MESSAGE
                        '',                  Expected the root token [VALUE] while evaluating [name]
                        invalidValue,        Expected the root token [VALUE] while evaluating [name]
                        VALUE..,             Found unexpected trailing dot(s) in the expression [VALUE..] while evaluating [name]
                        VALUE.a. .b,         Found the invalid expression [VALUE.a. .b] with missing tokens while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd,  Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],     Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
                        VALUE.attrib[a].,    Found unexpected trailing dot(s) in the expression [VALUE.attrib[a].] while evaluating [name]
                    """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,        EXPECTED_ERROR_MESSAGE
                        '',                Expected the root token [KEY] while evaluating [name]
                        invalidKey,        Expected the root token [KEY] while evaluating [name]
                        KEY..,             Found unexpected trailing dot(s) in the expression [KEY..] while evaluating [name]
                        KEY.a. .b,         Found the invalid expression [KEY.a. .b] with missing tokens while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[0]xsd,  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[a],     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
                        KEY.attrib[a].,    Found unexpected trailing dot(s) in the expression [KEY.attrib[a].] while evaluating [name]
                    """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> scalars() {
        return Stream.of(
                arguments(Schema.INT8_SCHEMA, 8, null),
                arguments(Schema.INT16_SCHEMA, 16, null),
                arguments(Schema.INT32_SCHEMA, 32, null),
                arguments(Schema.INT64_SCHEMA, 64, null),
                arguments(Schema.BOOLEAN_SCHEMA, true, null),
                arguments(Schema.BOOLEAN_SCHEMA, false, null),
                arguments(Schema.STRING_SCHEMA, "abcd", null),
                arguments(Schema.BYTES_SCHEMA, "abcd".getBytes(), "[97, 98, 99, 100]"),
                arguments(
                        Schema.BYTES_SCHEMA,
                        ByteBuffer.wrap("abcd".getBytes()),
                        "[97, 98, 99, 100]"));
    }

    @ParameterizedTest
    @MethodSource("scalars")
    public void shouldExtractFromScalar(Schema schema, Object value) {
        // Make a very generic SinkRecord by popoluating either key and value so that we can test
        // selectors for each
        // of them
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, value, schema, value, 0);
        KafkaRecord<Object, Object> record = KafkaRecord.from(sinkRecord);

        String expected =
                switch (value) {
                    case byte[] bytes -> Arrays.toString(bytes);
                    case ByteBuffer buffer -> Arrays.toString(buffer.array());
                    default -> value.toString();
                };
        assertThat(valueSelector("VALUE").extract(record).text()).isEqualTo(expected);
        assertThat(keySelector("KEY").extract(record).text()).isEqualTo(expected);
    }

    @Test
    public void shouldExtractFromFlatStruct() {
        Struct struct = makeFlatStruct();
        Schema schema = struct.schema();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> record = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.int8").extract(record).text()).isEqualTo("8");
        assertThat(valueSelector("VALUE.int16").extract(record).text()).isEqualTo("16");
        assertThat(valueSelector("VALUE.int32").extract(record).text()).isEqualTo("32");
        assertThat(valueSelector("VALUE.int64").extract(record).text()).isEqualTo("64");
        assertThat(valueSelector("VALUE.boolean").extract(record).text()).isEqualTo("true");
        assertThat(valueSelector("VALUE.string").extract(record).text()).isEqualTo("abcd");
        assertThat(valueSelector("VALUE.bytes").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(valueSelector("VALUE.byteBuffer").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");

        assertThat(keySelector("KEY.int8").extract(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.int16").extract(record).text()).isEqualTo("16");
        assertThat(keySelector("KEY.int32").extract(record).text()).isEqualTo("32");
        assertThat(keySelector("KEY.int64").extract(record).text()).isEqualTo("64");
        assertThat(keySelector("KEY.boolean").extract(record).text()).isEqualTo("true");
        assertThat(keySelector("KEY.string").extract(record).text()).isEqualTo("abcd");
        assertThat(keySelector("KEY.bytes").extract(record).text()).isEqualTo("[97, 98, 99, 100]");
        assertThat(keySelector("KEY.byteBuffer").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromNested() {
        Struct struct = new Struct(NESTED_SCHEMA).put("nested", makeFlatStruct());
        Schema schema = struct.schema();

        KafkaRecord<Object, Object> record =
                KafkaRecord.from(new SinkRecord("topic", 1, schema, struct, schema, struct, 0));

        assertThat(valueSelector("VALUE.nested.int8").extract(record).text()).isEqualTo("8");
        assertThat(valueSelector("VALUE.nested['int8']").extract(record).text()).isEqualTo("8");
        assertThat(valueSelector("VALUE.nested.byteBuffer").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(valueSelector("VALUE.nested['byteBuffer']").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");

        assertThat(keySelector("KEY.nested.int8").extract(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.nested['int8']").extract(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.nested.byteBuffer").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(keySelector("KEY.nested['byteBuffer']").extract(record).text())
                .isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA).put("map", Collections.singletonMap("key", "value"));
        Schema schema = struct.schema();

        KafkaRecord<Object, Object> record =
                KafkaRecord.from(new SinkRecord("topic", 1, schema, struct, schema, struct, 0));

        assertThat(valueSelector("VALUE.map['key']").extract(record).text()).isEqualTo("value");
        assertThat(keySelector("KEY.map['key']").extract(record).text()).isEqualTo("value");
    }

    @Test
    public void shouldExtractFromComplexMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put("complexMap", Collections.singletonMap("key", makeFlatStruct()));
        Schema schema = struct.schema();

        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.complexMap['key'].int8").extract(kafkaRecord).text())
                .isEqualTo("8");
        assertThat(valueSelector("VALUE.complexMap['key']['int8']").extract(kafkaRecord).text())
                .isEqualTo("8");
        assertThat(keySelector("KEY.complexMap['key'].int8").extract(kafkaRecord).text())
                .isEqualTo("8");
        assertThat(keySelector("KEY.complexMap['key']['int8']").extract(kafkaRecord).text())
                .isEqualTo("8");
    }

    @Test
    public void shouldExtractFromMapOfMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put(
                                "mapOfMap",
                                Collections.singletonMap(
                                        "key", Collections.singletonMap("key", "value")));

        Schema schema = struct.schema();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.mapOfMap['key']['key']").extract(kafkaRecord).text())
                .isEqualTo("value");
        assertThat(keySelector("KEY.mapOfMap['key']['key']").extract(kafkaRecord).text())
                .isEqualTo("value");
    }

    private static Struct makeFlatStruct() {
        return new Struct(OPTIONAL_FLAT_SCHEMA)
                .put("int8", (byte) 8)
                .put("int16", (short) 16)
                .put("int32", 32)
                .put("int64", (long) 64)
                .put("float32", 32.f)
                .put("float64", 64.d)
                .put("boolean", true)
                .put("string", "abcd")
                .put("bytes", "abcd".getBytes())
                .put("byteBuffer", ByteBuffer.wrap("abcd".getBytes()));
    }
}