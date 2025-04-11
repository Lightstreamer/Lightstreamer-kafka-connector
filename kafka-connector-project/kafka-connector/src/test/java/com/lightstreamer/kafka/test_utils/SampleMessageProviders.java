
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

package com.lightstreamer.kafka.test_utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.example.Person;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleMessageProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SampleMessageProviders {

    public interface SampleMessageProvider<T> {
        T sampleMessage();

        default T sampleMessageV2() {
            return sampleMessage();
        }
    }

    public static SampleMessageProvider<GenericRecord> SampleGenericRecordProvider() {
        return new GenericRecordProvider();
    }

    public static SampleMessageProvider<JsonNode> SampleJsonNodeProvider() {
        return new SampleJsonNodeProvider();
    }

    public static SampleMessageProvider<Struct> SampleStructProvider() {
        return new SchemaAndValueProvider();
    }

    public static SampleMessageProvider<DynamicMessage> SampleDynamicMessageProvider() {
        return new SampleDynamicMessageProvider();
    }

    private static class SampleDynamicMessageProvider
            implements SampleMessageProvider<DynamicMessage> {

        private final DynamicMessage message;

        private SampleDynamicMessageProvider() {
            Person person = Person.newBuilder().setName("joe").build();
            message = DynamicMessage.newBuilder(person).build();
        }

        @Override
        public DynamicMessage sampleMessage() {
            return message;
        }
    }

    private static class GenericRecordProvider implements SampleMessageProvider<GenericRecord> {

        private final Schema valueSchema;
        private final Schema childrenSchema;
        private final Schema emptyArraySchema;

        private GenericRecordProvider() {
            ClassLoader classLoader = GenericRecordProvider.class.getClassLoader();
            Schema.Parser parser = new Schema.Parser();

            try {
                valueSchema = parser.parse(classLoader.getResourceAsStream("value.avsc"));
                childrenSchema = valueSchema.getField("children").schema();
                emptyArraySchema = valueSchema.getField("emptyArray").schema();
            } catch (IOException io) {
                throw new RuntimeException(io);
            }
        }

        @Override
        public GenericRecord sampleMessageV2() {
            GenericRecord joe = new GenericData.Record(valueSchema);
            joe.put("name", "joe");

            Map<Utf8, String> preferences = new LinkedHashMap<>();
            preferences.put(new Utf8("pref1"), "pref_value1");
            preferences.put(new Utf8("pref2"), "pref_value2");
            joe.put("preferences", preferences);

            Schema enumSchema = valueSchema.getField("type").schema();
            joe.put("type", new GenericData.EnumSymbol(enumSchema, "TYPE1"));
            joe.put(
                    "signature",
                    new GenericData.Fixed(
                            valueSchema.getField("signature").schema(), "abcd".getBytes()));

            GenericRecord documentRecord =
                    new GenericData.Record(valueSchema.getField("main_document").schema());
            documentRecord.put("doc_id", "ID123");
            documentRecord.put("doc_type", "ID");
            joe.put("documents", Map.of(new Utf8("id"), documentRecord));
            joe.put("emptyArray", new GenericData.Array<>(emptyArraySchema, null));
            joe.put("nullArray", null);
            return joe;
        }

        @Override
        public GenericRecord sampleMessage() {
            GenericRecord parentJoe = sampleMessageV2();

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
    }

    private static class SampleJsonNodeProvider implements SampleMessageProvider<JsonNode> {

        @Override
        public JsonNode sampleMessage() {
            List<Value> joeChildren =
                    new ArrayList<>(
                            List.of(
                                    new Value("alex"),
                                    new Value(
                                            "anna",
                                            List.of(new Value("gloria"), new Value("terence"))),
                                    new Value("serena")));
            joeChildren.add(null);

            Value root = new Value("joe", joeChildren);
            // root.
            root.signature = new byte[] {97, 98, 99, 100};
            root.family =
                    new Value[][] {
                        {new Value("bro00"), new Value("bro01")},
                        {new Value("bro10"), new Value("bro11")}
                    };

            ObjectNode node = new ObjectMapper().valueToTree(root);
            return node;
        }

        private static class Value {

            @JsonProperty String name;

            @JsonProperty Value child;

            @JsonProperty List<Value> children;

            @JsonProperty final List<String> nullArray = null;

            @JsonProperty byte[] signature;

            @JsonProperty Value[][] family;

            Value(String name) {
                this.name = name;
            }

            Value(String name, List<Value> children) {
                this(name);
                this.children = children;
            }
        }
    }

    private static class SchemaAndValueProvider implements SampleMessageProvider<Struct> {

        private final org.apache.kafka.connect.data.Schema grandSonsSchema =
                SchemaBuilder.struct()
                        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();

        private final org.apache.kafka.connect.data.Schema childrenSchema =
                SchemaBuilder.struct()
                        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(
                                "signature",
                                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                        .field("children", SchemaBuilder.array(grandSonsSchema).optional().build())
                        .optional() // This allows to put null entries.
                        .build();

        @Override
        public Struct sampleMessageV2() {
            // Schema preferencesSchema =
            //         SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
            // Schema documentsSchema =
            //         SchemaBuilder.map(
            //                         Schema.STRING_SCHEMA,
            //                         SchemaBuilder.struct()
            //                                 .field("doc_id", Schema.STRING_SCHEMA)
            //                                 .field("doc_type", Schema.STRING_SCHEMA)
            //                                 .build())
            //                 .build();

            org.apache.kafka.connect.data.Schema rootSchema =
                    SchemaBuilder.struct()
                            .name("com.lightstreamer.kafka.connect.Value")
                            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                            .field(
                                    "signature",
                                    org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
                            // .field("preferences", preferencesSchema)
                            // .field("documents", documentsSchema)
                            .field(
                                    "children",
                                    SchemaBuilder.array(childrenSchema).optional().build())
                            .field(
                                    "nullArray",
                                    SchemaBuilder.array(
                                                    org.apache.kafka.connect.data.Schema
                                                            .STRING_SCHEMA)
                                            .optional()
                                            .build())
                            .build();
            Struct rootNode =
                    new Struct(rootSchema)
                            .put("name", "joe")
                            .put("signature", new byte[] {97, 98, 99, 100})
                            .put("children", new ArrayList<>())
                            .put("nullArray", null);
            rootNode.validate();
            return rootNode;
        }

        private List<Object> newChildren() {
            List<Object> joeChildren =
                    new ArrayList<>(
                            List.of(
                                    new Struct(childrenSchema).put("name", "alex"),
                                    new Struct(childrenSchema)
                                            .put("name", "anna")
                                            .put(
                                                    "children",
                                                    List.of(
                                                            new Struct(grandSonsSchema)
                                                                    .put("name", "gloria"),
                                                            new Struct(grandSonsSchema)
                                                                    .put("name", "terence"))),
                                    new Struct(childrenSchema).put("name", "serena")));
            joeChildren.add(null);
            return joeChildren;
        }

        @Override
        public Struct sampleMessage() {
            Struct rootNode = sampleMessageV2();
            rootNode.put("children", newChildren());
            rootNode.validate();
            return rootNode;
        }
    }
}
