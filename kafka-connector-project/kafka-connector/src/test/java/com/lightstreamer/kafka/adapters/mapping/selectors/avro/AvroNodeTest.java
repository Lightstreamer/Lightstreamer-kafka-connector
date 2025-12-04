
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AvroNodeTest {

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

        AvroNode node = AvroNode.newNode("rootNode", record);
        assertThat(node.name()).isEqualTo("rootNode");
        assertThat(node.isNull()).isFalse();
        assertThat(node.isArray()).isFalse();
        assertThat(node.isScalar()).isFalse();
        assertThat(node.size()).isEqualTo(0);

        assertThat(node.has("field1")).isTrue();
        AvroNode field1Node = node.getProperty("field1Node", "field1");
        assertThat(field1Node.name()).isEqualTo("field1Node");
        assertThat(field1Node.text()).isEqualTo("field1Value");

        assertThat(node.has("field2")).isTrue();
        AvroNode field2Node = node.getProperty("field2Node", "field2");
        assertThat(field2Node.name()).isEqualTo("field2Node");
        assertThat(field2Node.text()).isEqualTo("field2Value");

        Map<String, String> target = new HashMap<>();
        node.flatIntoMap(target);
        assertThat(target)
                .containsExactly(
                        "field1", "field1Value",
                        "field2", "field2Value");
        target.clear();
    }

    @Test
    public void shouldCreateAvroNodeFromNull() {
        AvroNode nullNode = AvroNode.newNode("nullNode", null);
        assertThat(nullNode.name()).isEqualTo("nullNode");
        assertThat(nullNode.isNull()).isTrue();
        assertThat(nullNode.isArray()).isFalse();
        assertThat(nullNode.isScalar()).isTrue();
        assertThat(nullNode.text()).isEqualTo(null);
        assertThat(nullNode.has("aField")).isFalse();
        assertThat(nullNode.size()).isEqualTo(0);

        Map<String, String> target = new HashMap<>();
        nullNode.flatIntoMap(target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @Test
    public void shouldCreateAvroNodeFromScalar() {
        AvroNode intNode = AvroNode.newNode("scalarNode", 12);
        assertThat(intNode.name()).isEqualTo("scalarNode");
        assertThat(intNode.isNull()).isFalse();
        assertThat(intNode.isArray()).isFalse();
        assertThat(intNode.isScalar()).isTrue();
        assertThat(intNode.text()).isEqualTo("12");
        assertThat(intNode.has("aField")).isFalse();
        assertThat(intNode.size()).isEqualTo(0);

        Map<String, String> target = new HashMap<>();
        intNode.flatIntoMap(target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @Test
    public void shouldCreateAvroNodeFromArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<>(3, schema);
        array.add(1);
        array.add(2);
        array.add(3);
        array.add(null);

        AvroNode arrayNode = AvroNode.newNode("arrayNode", array);
        assertThat(arrayNode.name()).isEqualTo("arrayNode");
        assertThat(arrayNode.isNull()).isFalse();
        assertThat(arrayNode.isArray()).isTrue();
        assertThat(arrayNode.isScalar()).isFalse();
        assertThat(arrayNode.size()).isEqualTo(4);
        assertThat(arrayNode.text()).isEqualTo("[1, 2, 3, null]");
        assertThat(arrayNode.has("aField")).isFalse();

        AvroNode node1 = arrayNode.getIndexed("node1", 0, "arrayNode");
        assertThat(node1.name()).isEqualTo("node1");
        assertThat(node1.text()).isEqualTo("1");

        AvroNode node2 = arrayNode.getIndexed("node2", 1, "arrayNode");
        assertThat(node2.name()).isEqualTo("node2");
        assertThat(node2.text()).isEqualTo("2");

        AvroNode node3 = arrayNode.getIndexed("node3", 2, "arrayNode");
        assertThat(node3.name()).isEqualTo("node3");
        assertThat(node3.text()).isEqualTo("3");

        AvroNode node4 = arrayNode.getIndexed("node4", 3, "arrayNode");
        assertThat(node4.name()).isEqualTo("node4");
        assertThat(node4.text()).isEqualTo(null);

        ValueException ve =
                assertThrows(ValueException.class, () -> arrayNode.getProperty("node", "aField"));
        assertThat(ve).hasMessageThat().contains("Field [aField] not found");

        ValueException ive =
                assertThrows(
                        ValueException.class, () -> arrayNode.getIndexed("node5", 5, "arrayNode"));
        assertThat(ive).hasMessageThat().contains("Field not found at index [5]");

        Map<String, String> target = new HashMap<>();
        arrayNode.flatIntoMap(target);
        assertThat(target)
                .containsExactly(
                        "arrayNode[0]", "1",
                        "arrayNode[1]", "2",
                        "arrayNode[2]", "3",
                        "arrayNode[3]", null);
        target.clear();
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

        AvroNode mapNode = AvroNode.newNode("mapNode", map);
        assertThat(mapNode.name()).isEqualTo("mapNode");
        assertThat(mapNode.isNull()).isFalse();
        assertThat(mapNode.isArray()).isFalse();
        assertThat(mapNode.isScalar()).isFalse();
        assertThat(mapNode.size()).isEqualTo(0);
        assertThat(mapNode.text()).isEqualTo("{k1: v1, k2: v2}");

        assertThat(mapNode.has("k1")).isTrue();
        AvroNode node1 = mapNode.getProperty("k1Node", "k1");
        assertThat(node1.name()).isEqualTo("k1Node");
        assertThat(node1.text()).isEqualTo("v1");

        assertThat(mapNode.has("k2")).isTrue();
        AvroNode node2 = mapNode.getProperty("k2Node", "k2");
        assertThat(node2.name()).isEqualTo("k2Node");
        assertThat(node2.text()).isEqualTo("v2");

        assertThat(mapNode.has("k3")).isFalse();

        Map<String, String> target = new HashMap<>();
        mapNode.flatIntoMap(target);
        assertThat(target).containsExactly("k1", "v1", "k2", "v2");
        target.clear();
    }

    @Test
    public void shouldCreateFromFixed() {
        Schema schema = Schema.createFixed("fixed", null, null, 5);
        GenericData.Fixed fixed = new GenericData.Fixed(schema, "abcd".getBytes());
        AvroNode fixedNode = AvroNode.newNode("fixedNode", fixed);

        assertThat(fixedNode.name()).isEqualTo("fixedNode");
        assertThat(fixedNode.isNull()).isFalse();
        assertThat(fixedNode.isArray()).isFalse();
        assertThat(fixedNode.isScalar()).isTrue();
        assertThat(fixedNode.text()).isEqualTo("[97, 98, 99, 100]");
        assertThat(fixedNode.size()).isEqualTo(0);
        assertThat(fixedNode.has("aField")).isFalse();

        Map<String, String> target = new HashMap<>();
        fixedNode.flatIntoMap(target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @Test
    public void shouldCreateFromEnum() {
        Schema schema = Schema.createEnum("enum", null, null, List.of("A", "B", "C"));
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schema, "A");
        AvroNode enumNode = AvroNode.newNode("enumNode", enumSymbol);

        assertThat(enumNode.name()).isEqualTo("enumNode");
        assertThat(enumNode.isNull()).isFalse();
        assertThat(enumNode.isArray()).isFalse();
        assertThat(enumNode.isScalar()).isTrue();
        assertThat(enumNode.text()).isEqualTo("A");
        assertThat(enumNode.size()).isEqualTo(0);
        assertThat(enumNode.has("aField")).isFalse();

        Map<String, String> target = new HashMap<>();
        enumNode.flatIntoMap(target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @Test
    public void shouldCreateFromComplexRecord() {
        GenericRecord record = SampleMessageProviders.SampleGenericRecordProvider().sampleMessage();
        Node<AvroNode> recordNode = AvroNode.newNode("complexRootNode", record);
        assertThat(recordNode.name()).isEqualTo("complexRootNode");
        assertThat(recordNode.isNull()).isFalse();
        assertThat(recordNode.isArray()).isFalse();
        assertThat(recordNode.isScalar()).isFalse();
        assertThat(recordNode.size()).isEqualTo(0);

        Map<String, String> target = new HashMap<>();
        recordNode.flatIntoMap(target);
        assertThat(target)
                .containsAtLeast(
                        "name", "joe",
                        "type", "TYPE1",
                        "signature", "[97, 98, 99, 100]");
        target.clear();

        Node<AvroNode> nameNode = recordNode.getProperty("nameNode", "name");
        assertThat(nameNode.name()).isEqualTo("nameNode");
        assertThat(nameNode.isScalar()).isTrue();
        assertThat(nameNode.text()).isEqualTo("joe");
        assertThat(nameNode.size()).isEqualTo(0);

        nameNode.flatIntoMap(target);
        assertThat(target).isEmpty();
        target.clear();

        AvroNode preferencesNode = recordNode.getProperty("preferencesNode", "preferences");
        assertThat(preferencesNode.name()).isEqualTo("preferencesNode");
        assertThat(preferencesNode.isScalar()).isFalse();
        assertThat(preferencesNode.isArray()).isFalse();
        assertThat(preferencesNode.size()).isEqualTo(0);
        assertThat(preferencesNode.text()).isEqualTo("{pref1: pref_value1, pref2: pref_value2}");

        preferencesNode.flatIntoMap(target);
        assertThat(target)
                .containsExactly(
                        "pref1", "pref_value1",
                        "pref2", "pref_value2");
        target.clear();
    }
}
