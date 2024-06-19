
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

package com.lightstreamer.kafka.connect;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.adapters.test_utils.SchemAndValueProvider;
import com.lightstreamer.kafka.connect.mapping.ConnectSelectorsSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.ValueException;
import com.lightstreamer.kafka.mapping.selectors.ValueSelector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.util.Collections;

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

    static ValueSelector<Object> valueSelector(String expression) {
        return ConnectSelectorsSuppliers.valueSelectorSupplier().newSelector("name", expression);
    }

    static KeySelector<Object> keySelector(String expression) {
        return ConnectSelectorsSuppliers.keySelectorSupplier().newSelector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                        EXPRESSION                            | EXPECTED_VALUE
                        VALUE.name                            | joe
                        VALUE.signature                       | [97, 98, 99, 100]
                        VALUE.children[0].name                | alex
                        VALUE.children[0]['name']             | alex
                        VALUE.children[0].signature           | NULL
                        VALUE.children[1].name                | anna
                        VALUE.children[2].name                | serena
#                       VALUE.children[3]                     | NULL
                        VALUE.children[1].children[0].name    | gloria
                        VALUE.children[1].children[1].name    | terence
                        VALUE.children[1].children[1]['name'] | terence
                        """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ValueSelector<Object> selector = valueSelector(expression);
        Struct RECORD = SchemAndValueProvider.VALUE;
        SinkRecord record = new SinkRecord("topic", 1, null, null, RECORD.schema(), RECORD, 0);
        StringSubject subject = assertThat(selector.extract(KafkaRecord.from(record)).text());
        if (expectedValue.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expectedValue);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                         EXPECTED_ERROR_MESSAGE
                        VALUE.no_attrib,                    Field [no_attrib] not found
                        VALUE.children[0].no_attrib,        Field [no_attrib] not found
                        VALUE.no_children[0],               Field [no_children] not found
                        VALUE.name[0],                      Current field is not indexed
                        VALUE.children,                     The expression [VALUE.children] must evaluate to a non-complex object
                        VALUE.children[0]['no_key'],        Field [no_key] not found
                        VALUE.children[0],                  The expression [VALUE.children[0]] must evaluate to a non-complex object
                        VALUE.children[3].name,             Field [name] not found
                        VALUE.children[4],                  Field not found at index [4]
                        VALUE.children[4].name,             Field not found at index [4]
                        """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueSelector<Object> selector = valueSelector(expression);
        Struct RECORD = SchemAndValueProvider.VALUE;
        SinkRecord record = new SinkRecord("topic", 1, null, null, RECORD.schema(), RECORD, 0);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> selector.extract(KafkaRecord.from(record)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        ESPRESSION,                        EXPECTED_ERROR_MESSAGE
                        '',                                Expected the root token [VALUE] while evaluating [name]
                        invalidValue,                      Expected the root token [VALUE] while evaluating [name]
                        VALUE,                             Found the invalid expression [VALUE] while evaluating [name]
                        VALUE.,                            Found the invalid expression [VALUE.] while evaluating [name]
                        VALUE..,                           Found the invalid expression [VALUE..] with missing tokens while evaluating [name]
                        VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd,                Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],                   Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
                        VALUE.attrib[a].,                  Found the invalid indexed expression [VALUE.attrib[a].] while evaluating [name]
                    """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldExtractFromScalar() {
        Struct struct = makeFlatStruct();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        ValueSelector<Object> selector = valueSelector("VALUE.int8");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("8");

        selector = valueSelector("VALUE.int16");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("16");

        selector = valueSelector("VALUE.int32");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("32");

        selector = valueSelector("VALUE.int64");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("64");

        selector = valueSelector("VALUE.boolean");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("true");

        selector = valueSelector("VALUE.string");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("abcd");

        selector = valueSelector("VALUE.bytes");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("[97, 98, 99, 100]");

        selector = valueSelector("VALUE.byteBuffer");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromNested() {
        Struct struct = new Struct(NESTED_SCHEMA).put("nested", makeFlatStruct());
        struct.validate();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        ValueSelector<Object> selector = valueSelector("VALUE.nested.int8");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("8");

        selector = valueSelector("VALUE.nested['int8']");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("8");

        selector = valueSelector("VALUE.nested.byteBuffer");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("[97, 98, 99, 100]");

        selector = valueSelector("VALUE.nested['byteBuffer']");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldExtractFromMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA).put("map", Collections.singletonMap("key", "value"));
        struct.validate();
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        ValueSelector<Object> selector = valueSelector("VALUE.map['key']");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("value");
    }

    @Test
    public void shouldExtractFromComplexMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put("complexMap", Collections.singletonMap("key", makeFlatStruct()));
        struct.validate();

        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        ValueSelector<Object> selector = valueSelector("VALUE.complexMap['key'].int8");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("8");

        selector = valueSelector("VALUE.complexMap['key']['int8']");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("8");
    }

    @Test
    public void shouldExtractFromMapOfMap() {
        Struct struct =
                new Struct(NESTED_SCHEMA)
                        .put(
                                "mapOfMap",
                                Collections.singletonMap(
                                        "key", Collections.singletonMap("key", "value")));
        struct.validate();

        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0);
        KafkaRecord<Object, Object> kafkaRecord = KafkaRecord.from(sinkRecord);

        ValueSelector<Object> selector = valueSelector("VALUE.mapOfMap['key']['key']");
        assertThat(selector.extract(kafkaRecord).text()).isEqualTo("value");
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
