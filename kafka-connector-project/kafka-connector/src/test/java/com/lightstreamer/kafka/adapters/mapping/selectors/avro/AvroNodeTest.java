
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
        AvroNode field1Node = node.get("field1");
        assertThat(field1Node.name()).isEqualTo("field1");
        assertThat(field1Node.asText()).isEqualTo("field1Value");

        assertThat(node.has("field2")).isTrue();
        AvroNode field2Node = node.get("field2");
        assertThat(field2Node.name()).isEqualTo("field2");
        assertThat(field2Node.asText()).isEqualTo("field2Value");

        List<AvroNode> fields = node.fields();
        assertThat(fields).hasSize(2);

        AvroNode field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("field1");
        assertThat(field1.asText()).isEqualTo("field1Value");

        AvroNode field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("field2");
        assertThat(field2.asText()).isEqualTo("field2Value");
    }

    @Test
    public void shouldCreateAvroNodeFromNull() {
        AvroNode nullNode = AvroNode.from("null", null);
        assertThat(nullNode.name()).isEqualTo("null");
        assertThat(nullNode.isNull()).isTrue();
        assertThat(nullNode.isContainer()).isFalse();
        assertThat(nullNode.isArray()).isFalse();
        assertThat(nullNode.isScalar()).isTrue();
        assertThat(nullNode.asText()).isEqualTo(null);
        assertThat(nullNode.has("aField")).isFalse();
        assertThat(nullNode.size()).isEqualTo(0);
        assertThat(nullNode.fields()).isEmpty();
    }

    @Test
    public void shouldCreateAvroNodeFromScalar() {
        AvroNode intNode = AvroNode.from("int", 12);
        assertThat(intNode.name()).isEqualTo("int");
        assertThat(intNode.isNull()).isFalse();
        assertThat(intNode.isContainer()).isFalse();
        assertThat(intNode.isArray()).isFalse();
        assertThat(intNode.isScalar()).isTrue();
        assertThat(intNode.asText()).isEqualTo("12");
        assertThat(intNode.has("aField")).isFalse();
        assertThat(intNode.size()).isEqualTo(0);
        assertThat(intNode.fields()).isEmpty();
    }

    @Test
    public void shouldCreateAvroNodeFromArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<>(3, schema);
        array.add(1);
        array.add(2);
        array.add(3);

        AvroNode arrayNode = AvroNode.from("array", array);
        assertThat(arrayNode.name()).isEqualTo("array");
        assertThat(arrayNode.isNull()).isFalse();
        assertThat(arrayNode.isContainer()).isTrue();
        assertThat(arrayNode.isArray()).isTrue();
        assertThat(arrayNode.isScalar()).isFalse();
        assertThat(arrayNode.size()).isEqualTo(3);
        assertThat(arrayNode.asText()).isEqualTo("[1, 2, 3]");
        assertThat(arrayNode.has("aField")).isFalse();

        AvroNode node1 = arrayNode.get(0);
        assertThat(node1.name()).isEqualTo("array[0]");
        assertThat(node1.asText()).isEqualTo("1");

        AvroNode node2 = arrayNode.get(1);
        assertThat(node2.name()).isEqualTo("array[1]");
        assertThat(node2.asText()).isEqualTo("2");

        AvroNode node3 = arrayNode.get(2);
        assertThat(node3.name()).isEqualTo("array[2]");
        assertThat(node3.asText()).isEqualTo("3");

        assertThrows(ClassCastException.class, () -> arrayNode.get("aField"));

        List<AvroNode> fields = arrayNode.fields();
        assertThat(fields).hasSize(3);

        AvroNode field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("array[0]");
        assertThat(field1.asText()).isEqualTo("1");

        AvroNode field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("array[1]");
        assertThat(field2.asText()).isEqualTo("2");

        AvroNode field3 = fields.get(2);
        assertThat(field3.name()).isEqualTo("array[2]");
        assertThat(field3.asText()).isEqualTo("3");
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
        AvroNode node1 = mapNode.get("k1");
        assertThat(node1.name()).isEqualTo("k1");
        assertThat(node1.asText()).isEqualTo("v1");

        assertThat(mapNode.has("k2")).isTrue();
        AvroNode node2 = mapNode.get("k2");
        assertThat(node2.name()).isEqualTo("k2");
        assertThat(node2.asText()).isEqualTo("v2");

        assertThat(mapNode.has("k3")).isFalse();

        List<AvroNode> fields = mapNode.fields();
        assertThat(fields.size()).isEqualTo(2);
        AvroNode field1 = fields.get(0);
        assertThat(field1.name()).isEqualTo("k1");
        assertThat(field1.asText()).isEqualTo("v1");

        AvroNode field2 = fields.get(1);
        assertThat(field2.name()).isEqualTo("k2");
        assertThat(field2.asText()).isEqualTo("v2");
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
        assertThat(fixedNode.asText()).isEqualTo("[97, 98, 99, 100]");
        assertThat(fixedNode.size()).isEqualTo(0);
        assertThat(fixedNode.has("aField")).isFalse();

        assertThat(fixedNode.fields()).isEmpty();
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
        assertThat(enumNode.asText()).isEqualTo("A");
        assertThat(enumNode.size()).isEqualTo(0);
        assertThat(enumNode.has("aField")).isFalse();

        assertThat(enumNode.fields()).isEmpty();
    }

    public void shouldCreateFromComplexRecord() {
        
    }
}
