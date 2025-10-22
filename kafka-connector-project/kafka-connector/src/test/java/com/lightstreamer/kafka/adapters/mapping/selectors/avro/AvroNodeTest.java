
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers.AvroNode;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public class AvroNodeTest {
    // Test cases for AvroNode

    @Test
    public void shouldCreateAvroNodeFromSimpleRecord() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                """
                    {
                    "type": "record",
                    "name": "TestRecord",
                    "fields": [
                        {
                        "name": "field1",
                        "type": "string"
                        },
                        {
                        "name": "field2",
                        "type": "int"
                        }
                    ]
                    }
                """);

        GenericRecord record = new GenericData.Record(schema);
        record.put("field1", "field1Value");
        record.put("field2", "field2Value");

        AvroNode node = AvroNode.from("root", record);
        assertThat(node.name()).isEqualTo("root");
        assertThat(node.isNull()).isFalse();
        assertThat(node.isContainer()).isTrue();
        assertThat(node.isArray()).isFalse();
        assertThat(node.isScalar()).isFalse();
        assertThat(node.size()).isEqualTo(0);

        assertThat(node.has("field1")).isTrue();
        AvroNode field1Node = node.getProperty("field1");
        assertThat(field1Node.name()).isEqualTo("field1");
        assertThat(field1Node.text()).isEqualTo("field1Value");

        assertThat(node.has("field2")).isTrue();
        AvroNode field2Node = node.getProperty("field2");
        assertThat(field2Node.name()).isEqualTo("field2");
        assertThat(field2Node.text()).isEqualTo("field2Value");

        List<Data> fields = node.toData();
        assertThat(fields).hasSize(2);

        Data field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("field1");
        assertThat(field1.text()).isEqualTo("field1Value");

        Data field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("field2");
        assertThat(field2.text()).isEqualTo("field2Value");
    }

    @Test
    public void shouldCreateAvroNodeFromNull() {
        AvroNode nullNode = AvroNode.from("null", null);
        assertThat(nullNode.name()).isEqualTo("null");
        assertThat(nullNode.isNull()).isTrue();
        assertThat(nullNode.isContainer()).isFalse();
        assertThat(nullNode.isArray()).isFalse();
        assertThat(nullNode.isScalar()).isTrue();
        assertThat(nullNode.text()).isEqualTo(null);
        assertThat(nullNode.has("aField")).isFalse();
        assertThat(nullNode.size()).isEqualTo(0);

        List<Data> data = nullNode.toData();
        assertThat(data).hasSize(1);
        Data nullData = data.get(0);
        assertThat(nullData.name()).isEqualTo("null");
        assertThat(nullData.text()).isEqualTo(null);
    }

    @Test
    public void shouldCreateAvroNodeFromScalar() {
        AvroNode intNode = AvroNode.from("int", 12);
        assertThat(intNode.name()).isEqualTo("int");
        assertThat(intNode.isNull()).isFalse();
        assertThat(intNode.isContainer()).isFalse();
        assertThat(intNode.isArray()).isFalse();
        assertThat(intNode.isScalar()).isTrue();
        assertThat(intNode.text()).isEqualTo("12");
        assertThat(intNode.has("aField")).isFalse();
        assertThat(intNode.size()).isEqualTo(0);

        List<Data> data = intNode.toData();
        assertThat(data).hasSize(1);
        Data intData = data.get(0);
        assertThat(intData.name()).isEqualTo("int");
        assertThat(intData.text()).isEqualTo("12");
    }

    @Test
    public void shouldCreateAvroNodeFromArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<>(3, schema);
        array.add(1);
        array.add(2);
        array.add(3);
        array.add(null);

        AvroNode arrayNode = AvroNode.from("array", array);
        assertThat(arrayNode.name()).isEqualTo("array");
        assertThat(arrayNode.isNull()).isFalse();
        assertThat(arrayNode.isContainer()).isTrue();
        assertThat(arrayNode.isArray()).isTrue();
        assertThat(arrayNode.isScalar()).isFalse();
        assertThat(arrayNode.size()).isEqualTo(4);
        assertThat(arrayNode.text()).isEqualTo("[1, 2, 3, null]");
        assertThat(arrayNode.has("aField")).isFalse();

        AvroNode node1 = arrayNode.getIndexed(0);
        assertThat(node1.name()).isEqualTo("array[0]");
        assertThat(node1.text()).isEqualTo("1");

        AvroNode node2 = arrayNode.getIndexed(1);
        assertThat(node2.name()).isEqualTo("array[1]");
        assertThat(node2.text()).isEqualTo("2");

        AvroNode node3 = arrayNode.getIndexed(2);
        assertThat(node3.name()).isEqualTo("array[2]");
        assertThat(node3.text()).isEqualTo("3");

        AvroNode node4 = arrayNode.getIndexed(3);
        assertThat(node4.name()).isEqualTo("array[3]");
        assertThat(node4.text()).isEqualTo(null);

        ValueException ve =
                assertThrows(ValueException.class, () -> arrayNode.getProperty("aField"));
        assertThat(ve).hasMessageThat().contains("Field [aField] not found");

        List<Data> fields = arrayNode.toData();
        assertThat(fields).hasSize(4);

        Data field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("array[0]");
        assertThat(field1.text()).isEqualTo("1");

        Data field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("array[1]");
        assertThat(field2.text()).isEqualTo("2");

        Data field3 = fields.get(2);
        assertThat(field3.name()).isEqualTo("array[2]");
        assertThat(field3.text()).isEqualTo("3");

        Data field4 = fields.get(3);
        assertThat(field4.name()).isEqualTo("array[3]");
        assertThat(field4.text()).isEqualTo(null);
    }

    static Utf8 utf8(String s) {
        return new Utf8(s);
    }

    @Test
    public void shouldCreateAvroNodeFromMap() throws IOException {
        // Use LinkedHashMap to preserve insertion order
        LinkedHashMap<Utf8, String> map = new LinkedHashMap<>();
        map.put(utf8("k1"), "v1");
        map.put(utf8("k2"), "v2");

        AvroNode mapNode = AvroNode.from("map", map);
        assertThat(mapNode.name()).isEqualTo("map");
        assertThat(mapNode.isNull()).isFalse();
        assertThat(mapNode.isContainer()).isFalse();
        assertThat(mapNode.isArray()).isFalse();
        assertThat(mapNode.isScalar()).isFalse();
        assertThat(mapNode.size()).isEqualTo(0);
        // assertThat(mapNode.asText()).isEqualTo("{k1=v1, k2=v2}");

        assertThat(mapNode.has("k1")).isTrue();
        AvroNode node1 = mapNode.getProperty("k1");
        assertThat(node1.name()).isEqualTo("k1");
        assertThat(node1.text()).isEqualTo("v1");

        assertThat(mapNode.has("k2")).isTrue();
        AvroNode node2 = mapNode.getProperty("k2");
        assertThat(node2.name()).isEqualTo("k2");
        assertThat(node2.text()).isEqualTo("v2");

        assertThat(mapNode.has("k3")).isFalse();

        List<Data> fields = mapNode.toData();
        assertThat(fields.size()).isEqualTo(2);
        Data field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("k1");
        assertThat(field1.text()).isEqualTo("v1");

        Data field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("k2");
        assertThat(field2.text()).isEqualTo("v2");
    }

    @Test
    public void shouldCreateFromFixed() {
        Schema schema = Schema.createFixed("fixed", null, null, 5);
        GenericData.Fixed fixed = new GenericData.Fixed(schema, "abcd".getBytes());
        AvroNode fixedNode = AvroNode.from("fixed", fixed);

        assertThat(fixedNode.name()).isEqualTo("fixed");
        assertThat(fixedNode.isNull()).isFalse();
        assertThat(fixedNode.isContainer()).isTrue();
        assertThat(fixedNode.isArray()).isFalse();
        assertThat(fixedNode.isScalar()).isTrue();
        assertThat(fixedNode.text()).isEqualTo("[97, 98, 99, 100]");
        assertThat(fixedNode.size()).isEqualTo(0);
        assertThat(fixedNode.has("aField")).isFalse();

        List<Data> data = fixedNode.toData();
        assertThat(data).hasSize(1);
        Data fixedData = data.get(0);
        assertThat(fixedData.name()).isEqualTo("fixed");
        assertThat(fixedData.text()).isEqualTo("[97, 98, 99, 100]");
    }

    @Test
    public void shouldCreateFromEnum() {
        Schema schema = Schema.createEnum("enum", null, null, List.of("A", "B", "C"));
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schema, "A");
        AvroNode enumNode = AvroNode.from("enum", enumSymbol);

        assertThat(enumNode.name()).isEqualTo("enum");
        assertThat(enumNode.isNull()).isFalse();
        assertThat(enumNode.isContainer()).isTrue();
        assertThat(enumNode.isArray()).isFalse();
        assertThat(enumNode.isScalar()).isTrue();
        assertThat(enumNode.text()).isEqualTo("A");
        assertThat(enumNode.size()).isEqualTo(0);
        assertThat(enumNode.has("aField")).isFalse();

        List<Data> data = enumNode.toData();
        assertThat(data).hasSize(1);
        Data enumData = data.get(0);
        assertThat(enumData.name()).isEqualTo("enum");
        assertThat(enumData.text()).isEqualTo("A");
    }

    @Test
    public void shouldCreateFromComplexRecord() {
        GenericRecord record = SampleMessageProviders.SampleGenericRecordProvider().sampleMessage();
        Node<AvroNode> recordNode = AvroNode.from("root", record);
        assertThat(recordNode.name()).isEqualTo("root");
        assertThat(recordNode.isNull()).isFalse();
        assertThat(recordNode.isArray()).isFalse();
        assertThat(recordNode.isScalar()).isFalse();
        assertThat(recordNode.size()).isEqualTo(0);

        Node<AvroNode> nameNode = recordNode.getProperty("name");
        assertThat(nameNode.name()).isEqualTo("name");
        assertThat(nameNode.isScalar()).isTrue();
        assertThat(nameNode.text()).isEqualTo("joe");
        assertThat(nameNode.size()).isEqualTo(0);

        AvroNode preferencesNode = recordNode.getProperty("preferences");
        assertThat(preferencesNode.name()).isEqualTo("preferences");
        assertThat(preferencesNode.isScalar()).isFalse();
        assertThat(preferencesNode.isArray()).isFalse();
        assertThat(preferencesNode.size()).isEqualTo(0);

        List<Data> fields = preferencesNode.toData();
        assertThat(fields).hasSize(2);

        Data prefNode1 = fields.get(0);
        assertThat(prefNode1.name()).isEqualTo("pref1");
        assertThat(prefNode1.text()).isEqualTo("pref_value1");

        Data prefNode2 = fields.get(1);
        assertThat(prefNode2.name()).isEqualTo("pref2");
        assertThat(prefNode2.text()).isEqualTo("pref_value2");
    }
}
