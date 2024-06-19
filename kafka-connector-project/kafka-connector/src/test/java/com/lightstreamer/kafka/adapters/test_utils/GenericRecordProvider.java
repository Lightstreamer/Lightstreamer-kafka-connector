
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

package com.lightstreamer.kafka.adapters.test_utils;

import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorTest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GenericRecordProvider {

    private final Schema valueSchema;

    private final Schema childrenSchema;

    private GenericRecordProvider() {
        ClassLoader classLoader = GenericRecordSelectorTest.class.getClassLoader();
        Schema.Parser parser = new Schema.Parser();

        try {
            valueSchema = parser.parse(classLoader.getResourceAsStream("value.avsc"));
            childrenSchema = valueSchema.getField("children").schema();

        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    private static GenericRecordProvider PROVIDER = new GenericRecordProvider();

    public static GenericRecord RECORD = PROVIDER.newGenericRecord();

    private GenericRecord newGenericRecord() {
        GenericRecord parentJoe = new GenericData.Record(valueSchema);
        parentJoe.put("name", "joe");
        parentJoe.put(
                "preferences",
                Map.of(new Utf8("pref1"), "pref_value1", new Utf8("pref2"), "pref_value2"));
        Schema enumSchema = valueSchema.getField("type").schema();
        parentJoe.put("type", new GenericData.EnumSymbol(enumSchema, "TYPE1"));

        parentJoe.put(
                "signature",
                new GenericData.Fixed(
                        valueSchema.getField("signature").schema(), "abcd".getBytes()));

        GenericRecord documentRecord =
                new GenericData.Record(valueSchema.getField("main_document").schema());
        documentRecord.put("doc_id", "ID123");
        documentRecord.put("doc_type", "ID");
        parentJoe.put("documents", Map.of(new Utf8("id"), documentRecord));

        GenericRecord childAlex = new GenericData.Record(valueSchema);
        childAlex.put("name", "alex");

        GenericRecord childAnna = new GenericData.Record(valueSchema);
        childAnna.put("name", "anna");

        GenericRecord childSerena = new GenericData.Record(valueSchema);
        childSerena.put("name", "serena");

        GenericRecord childGloria = new GenericData.Record(valueSchema);
        childGloria.put("name", "gloria");

        GenericRecord childTerence = new GenericData.Record(valueSchema);
        childTerence.put("name", "terence");
        childAnna.put(
                "children",
                new GenericData.Array<>(childrenSchema, List.of(childGloria, childTerence)));

        ArrayList<GenericRecord> joeChildren =
                new ArrayList<>(List.of(childAlex, childAnna, childSerena));
        joeChildren.add(null);
        parentJoe.put("children", new GenericData.Array<>(childrenSchema, joeChildren));
        return parentJoe;
    }

    public static void main(String[] args) {
        System.out.println(RECORD);
    }
}
