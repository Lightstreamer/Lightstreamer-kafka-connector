
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
import static com.lightstreamer.kafka.test_utils.Records.sinkFromKey;
import static com.lightstreamer.kafka.test_utils.Records.sinkFromValue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
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

    static Struct STRUCT = SampleMessageProviders.SampleStructProvider().sampleMessage();
    static Struct SIMPLE_STRUCT = SampleMessageProviders.SampleStructProvider().sampleMessageV2();

    private ConnectSelectorsSuppliers connectSelectorsSuppliers;

    ValueSelector<Object> valueSelector(String expression) throws ExtractionException {
        return connectSelectorsSuppliers
                .valueSelectorSupplier()
                .newSelector("name", Expressions.Expression(expression));
    }

    KeySelector<Object> keySelector(String expression) throws ExtractionException {
        return connectSelectorsSuppliers
                .keySelectorSupplier()
                .newSelector("name", Expressions.Expression(expression));
    }

    @BeforeEach
    public void beforeEach() {
        connectSelectorsSuppliers = new ConnectSelectorsSuppliers();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input VALUE.signature
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
    public void shouldExtractValue(String expression, String expected) throws ExtractionException {
        StringSubject subject =
                assertThat(
                        valueSelector(expression)
                                .extractValue(sinkFromValue("topic", STRUCT.schema(), STRUCT))
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
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION     |  EXPECTED
                        VALUE          |  {"name":"joe","signature":"YWJjZA==","children":[],"nullArray":null}
                        VALUE.children | []
                        VALUE.name     | joe
                        """)
    public void shouldExtractValueWithNonScalars(String expression, String expected)
            throws ExtractionException {
        String text =
                valueSelector(expression)
                        .extractValue(
                                sinkFromValue("topic", SIMPLE_STRUCT.schema(), SIMPLE_STRUCT),
                                false)
                        .text();
        assertThat(text).isEqualTo(expected);
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
                        VALUE.name['no_key'],         Cannot retrieve field [no_key] from a scalar object
                        VALUE.name.no_key,            Cannot retrieve field [no_key] from a scalar object
                        VALUE.children,               The expression [VALUE.children] must evaluate to a non-complex object
                        VALUE.children[0]['no_key'],  Field [no_key] not found
                        VALUE.children[0],            The expression [VALUE.children[0]] must evaluate to a non-complex object
                        VALUE.children[3].name,       Cannot retrieve field [name] from a null object
                        VALUE.children[4],            Field not found at index [4]
                        VALUE.children[4].name,       Field not found at index [4]
                        VALUE.nullArray[0],           Cannot retrieve index [0] from null object [nullArray]
                        """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue(
                                                sinkFromValue("topic", STRUCT.schema(), STRUCT)));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @Test
    public void shouldNotExtractValueDueToMissingSchema() {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector("VALUE")
                                        .extractValue(sinkFromValue("topic", null, "a Value")));
        assertThat(ve.getMessage()).isEqualTo("A Schema is required");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input KEY.signature
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
    public void shouldExtractKey(String expression, String expected) throws ExtractionException {
        StringSubject subject =
                assertThat(
                        keySelector(expression)
                                .extractKey(sinkFromKey("topic", STRUCT.schema(), STRUCT))
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
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION   |  EXPECTED
                        KEY          |  {"name":"joe","signature":"YWJjZA==","children":[],"nullArray":null}
                        KEY.children |  []
                        KEY.name     |  joe
                        """)
    public void shouldExtractKeyWithNonScalars(String expression, String expected)
            throws ExtractionException {
        String text =
                keySelector(expression)
                        .extractKey(
                                sinkFromKey("topic", SIMPLE_STRUCT.schema(), SIMPLE_STRUCT), false)
                        .text();
        assertThat(text).isEqualTo(expected);
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
                        KEY.name['no_key'],         Cannot retrieve field [no_key] from a scalar object
                        KEY.name.no_key,            Cannot retrieve field [no_key] from a scalar object
                        KEY.children,               The expression [KEY.children] must evaluate to a non-complex object
                        KEY.children[0]['no_key'],  Field [no_key] not found
                        KEY.children[0],            The expression [KEY.children[0]] must evaluate to a non-complex object
                        KEY.children[3].name,       Cannot retrieve field [name] from a null object
                        KEY.children[4],            Field not found at index [4]
                        KEY.children[4].name,       Field not found at index [4]
                        KEY.nullArray[0],           Cannot retrieve index [0] from null object [nullArray]
                        """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey(sinkFromKey("topic", STRUCT.schema(), STRUCT)));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,          EXPECTED_ERROR_MESSAGE
                        VALUE.a. .b,         Found the invalid expression [VALUE.a. .b] with missing tokens while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd,  Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],     Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
                    """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,        EXPECTED_ERROR_MESSAGE
                        KEY.a. .b,         Found the invalid expression [KEY.a. .b] with missing tokens while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[0]xsd,  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[a],     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
                    """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
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
    public void shouldExtractFromScalar(Schema schema, Object value) throws ExtractionException {
        // Make a very generic SinkRecord by populating either key and value so that we can test
        // selectors for each
        // of them
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, value, schema, value, 0);
        KafkaRecord<Object, Object> record = KafkaRecord.from(sinkRecord);

        // Valid ony on JDK 21
        // String expected =
        //         switch (value) {
        //             case byte[] bytes -> Arrays.toString(bytes);
        //             case ByteBuffer buffer -> Arrays.toString(buffer.array());
        //             default -> value.toString();
        //         };
        String expected;
        if (value instanceof byte[] bytes) {
            expected = Arrays.toString(bytes);
        } else if (value instanceof ByteBuffer buffer) {
            expected = Arrays.toString(buffer.array());
        } else {
            expected = value.toString();
        }
        assertThat(valueSelector("VALUE").extractValue(record).text()).isEqualTo(expected);
        assertThat(keySelector("KEY").extractKey(record).text()).isEqualTo(expected);
    }

    @Test
    public void shouldExtractFromFlatStruct() throws ExtractionException {
        Struct struct = makeFlatStruct();
        Schema schema = struct.schema();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> record = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.int8").extractValue(record).text()).isEqualTo("8");
        assertThat(valueSelector("VALUE.int16").extractValue(record).text()).isEqualTo("16");
        assertThat(valueSelector("VALUE.int32").extractValue(record).text()).isEqualTo("32");
        assertThat(valueSelector("VALUE.int64").extractValue(record).text()).isEqualTo("64");
        assertThat(valueSelector("VALUE.boolean").extractValue(record).text()).isEqualTo("true");
        assertThat(valueSelector("VALUE.string").extractValue(record).text()).isEqualTo("abcd");
        assertThat(valueSelector("VALUE.bytes").extractValue(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(valueSelector("VALUE.byteBuffer").extractValue(record).text())
                .isEqualTo("[97, 98, 99, 100]");

        assertThat(keySelector("KEY.int8").extractKey(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.int16").extractKey(record).text()).isEqualTo("16");
        assertThat(keySelector("KEY.int32").extractKey(record).text()).isEqualTo("32");
        assertThat(keySelector("KEY.int64").extractKey(record).text()).isEqualTo("64");
        assertThat(keySelector("KEY.boolean").extractKey(record).text()).isEqualTo("true");
        assertThat(keySelector("KEY.string").extractKey(record).text()).isEqualTo("abcd");
        assertThat(keySelector("KEY.bytes").extractKey(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(keySelector("KEY.byteBuffer").extractKey(record).text())
                .isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromNested() throws ExtractionException {
        Struct struct = new Struct(NESTED_SCHEMA).put("nested", makeFlatStruct());
        struct.validate();
        Schema schema = struct.schema();

        KafkaRecord<Object, Object> record =
                KafkaRecord.from(new SinkRecord("topic", 1, schema, struct, schema, struct, 0));

        assertThat(valueSelector("VALUE.nested.int8").extractValue(record).text()).isEqualTo("8");
        assertThat(valueSelector("VALUE.nested['int8']").extractValue(record).text())
                .isEqualTo("8");
        assertThat(valueSelector("VALUE.nested.byteBuffer").extractValue(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(valueSelector("VALUE.nested['byteBuffer']").extractValue(record).text())
                .isEqualTo("[97, 98, 99, 100]");

        assertThat(keySelector("KEY.nested.int8").extractKey(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.nested['int8']").extractKey(record).text()).isEqualTo("8");
        assertThat(keySelector("KEY.nested.byteBuffer").extractKey(record).text())
                .isEqualTo("[97, 98, 99, 100]");
        assertThat(keySelector("KEY.nested['byteBuffer']").extractKey(record).text())
                .isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromMap() throws ExtractionException {
        Struct struct =
                new Struct(NESTED_SCHEMA).put("map", Collections.singletonMap("key", "value"));
        struct.validate();
        Schema schema = struct.schema();

        KafkaRecord<Object, Object> record =
                KafkaRecord.from(new SinkRecord("topic", 1, schema, struct, schema, struct, 0));

        assertThat(valueSelector("VALUE.map['key']").extractValue(record).text())
                .isEqualTo("value");
        assertThat(keySelector("KEY.map['key']").extractKey(record).text()).isEqualTo("value");
    }

    @Test
    public void shouldExtractFromComplexMap() throws ExtractionException {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put("complexMap", Collections.singletonMap("key", makeFlatStruct()));
        struct.validate();
        Schema schema = struct.schema();

        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.complexMap['key'].int8").extractValue(kafkaRecord).text())
                .isEqualTo("8");
        assertThat(
                        valueSelector("VALUE.complexMap['key']['int8']")
                                .extractValue(kafkaRecord)
                                .text())
                .isEqualTo("8");
        assertThat(keySelector("KEY.complexMap['key'].int8").extractKey(kafkaRecord).text())
                .isEqualTo("8");
        assertThat(keySelector("KEY.complexMap['key']['int8']").extractKey(kafkaRecord).text())
                .isEqualTo("8");
    }

    @Test
    public void shouldExtractFromMapOfMap() throws ExtractionException {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put(
                                "mapOfMap",
                                Collections.singletonMap(
                                        "key", Collections.singletonMap("key", "value")));
        struct.validate();

        Schema schema = struct.schema();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, schema, struct, schema, struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        assertThat(valueSelector("VALUE.mapOfMap['key']['key']").extractValue(kafkaRecord).text())
                .isEqualTo("value");
        assertThat(keySelector("KEY.mapOfMap['key']['key']").extractKey(kafkaRecord).text())
                .isEqualTo("value");
    }

    private static Struct makeFlatStruct() {
        Struct struct =
                new Struct(OPTIONAL_FLAT_SCHEMA)
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
        struct.validate();
        return struct;
    }
}
