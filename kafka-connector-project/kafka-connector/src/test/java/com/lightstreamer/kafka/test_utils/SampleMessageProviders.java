
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
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.example.Address;
import com.lightstreamer.example.Car;
import com.lightstreamer.example.Job;
import com.lightstreamer.example.Person;

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

/**
 * A utility class that provides sample messages of different types for testing purposes.
 *
 * <p>This class offers providers for various message formats commonly used in Kafka applications:
 *
 * <ul>
 *   <li>Avro {@link org.apache.avro.generic.GenericRecord}
 *   <li>Jackson {@link com.fasterxml.jackson.databind.JsonNode}
 *   <li>Protobuf {@link com.google.protobuf.DynamicMessage}
 *   <li>Kafka Connect {@link org.apache.kafka.connect.data.Struct}
 * </ul>
 *
 * <p>Each provider implements the {@link SampleMessageProvider} interface, which defines methods to
 * obtain sample messages with hierarchical structures including arrays, maps, and nested objects.
 *
 * <p>Usage example:
 *
 * <pre>
 * // Get a sample Avro GenericRecord
 * GenericRecord record = SampleMessageProviders.SampleGenericRecordProvider().sampleMessage();
 *
 * // Get a sample JSON node
 * JsonNode json = SampleMessageProviders.SampleJsonNodeProvider().sampleMessage();
 * </pre>
 */
public class SampleMessageProviders {

    /**
     * Provides sample messages of a specific type for testing or demonstration purposes.
     *
     * <p>This interface defines methods for generating sample messages of type T that can be used
     * in unit testing.
     *
     * <p>The interface supports different versions of sample messages through dedicated methods,
     * allowing for testing with different message formats or schemas.
     *
     * @param <T> the type of message this provider generates
     */
    public interface SampleMessageProvider<T> {
        /**
         * Provides a sample message of type T.
         *
         * <p>This method is used to generate a representative sample message that can be used for
         * testing, demonstration, or as a template. The actual content of the message depends on
         * the implementing class.
         *
         * @return a sample message instance of type T
         */
        T sampleMessage();

        /**
         * Provides a version 2 sample message.
         *
         * <p>This default method currently returns the same instance as {@link #sampleMessage()},
         * but implementations can override it to provide a different message format for version 2
         * specific tests.
         *
         * @return a sample message of type T formatted according to version 2 requirements
         */
        default T sampleMessageV2() {
            return sampleMessage();
        }
    }

    private static final SampleMessageProvider<GenericRecord> GENERIC_RECORD_PROVIDER =
            new GenericRecordProvider();

    private static final SampleJsonNodeProvider JSON_NODE_PROVIDER = new SampleJsonNodeProvider();

    private static final SampleMessageProvider<DynamicMessage> DYNAMIC_MESSAGE_PROVIDER =
            new SampleDynamicMessageProvider();

    private static final SchemaAndValueProvider SCHEMA_AND_VALUE_PROVIDER =
            new SchemaAndValueProvider();

    /**
     * Returns a singleton instance of a {@link SampleMessageProvider} for {@link GenericRecord}
     * objects.
     *
     * @return a sample message provider for {@link GenericRecord} objects
     */
    public static SampleMessageProvider<GenericRecord> SampleGenericRecordProvider() {
        return GENERIC_RECORD_PROVIDER;
    }

    /**
     * Returns a singleton instance of a {@link SampleMessageProvider} for {@link JsonNode} objects.
     *
     * @return a sample message provider for {@link JsonNode} objects
     */
    public static SampleMessageProvider<JsonNode> SampleJsonNodeProvider() {
        return JSON_NODE_PROVIDER;
    }

    /**
     * Returns a singleton instance of a sample message provider for {@link DynamicMessage} objects.
     *
     * @return a sample message provider that creates {@link DynamicMessage} instances
     */
    public static SampleMessageProvider<DynamicMessage> SampleDynamicMessageProvider() {
        return DYNAMIC_MESSAGE_PROVIDER;
    }

    public static SampleMessageProvider<Struct> SampleStructProvider() {
        return SCHEMA_AND_VALUE_PROVIDER;
    }

    /**
     * Implementation of {@link SampleMessageProvider} that provides a sample {@link DynamicMessage}
     * containing a complex Protocol Buffer message structure representing a Person.
     *
     * <p>The sample message includes various Protocol Buffers data types and structures including:
     *
     * <ul>
     *   <li>Simple string fields (name, simpleRoleName)
     *   <li>Enumeration (job)
     *   <li>Repeated fields (phoneNumbers, friends)
     *   <li>Map fields (otherAddresses)
     *   <li>Nested messages (Address, Car)
     *   <li>ByteString (signature)
     * </ul>
     */
    private static class SampleDynamicMessageProvider
            implements SampleMessageProvider<DynamicMessage> {

        private final DynamicMessage message;

        private SampleDynamicMessageProvider() {
            Person person =
                    Person.newBuilder()
                            .setName("joe")
                            .setJob(Job.EMPLOYEE)
                            .setSimpleRoleName("Software Architect")
                            .addPhoneNumbers("012345")
                            .addPhoneNumbers("123456")
                            .addFriends(Person.newBuilder().setName("mike").build())
                            .addFriends(Person.newBuilder().setName("john").build())
                            .putOtherAddresses(
                                    "work",
                                    Address.newBuilder().setCity("Milan").setZip("20124").build())
                            .putOtherAddresses(
                                    "club",
                                    Address.newBuilder()
                                            .setCity("Siracusa")
                                            .setZip("96100")
                                            .build())
                            .putIndexedAddresses(1, Address.newBuilder().setCity("Rome").build())
                            .putBooleanAddresses(
                                    true, Address.newBuilder().setCity("Turin").build())
                            .putBooleanAddresses(
                                    false, Address.newBuilder().setCity("Florence").build())
                            .putData("data", -13.3f)
                            .setSignature(ByteString.copyFromUtf8("abcd"))
                            .setCar(Car.newBuilder().setBrand("BMW").build())
                            .setAny(Any.pack(Car.newBuilder().setBrand("FORD").build()))
                            .build();

            message = DynamicMessage.newBuilder(person).build();
        }

        @Override
        public DynamicMessage sampleMessage() {
            return message;
        }
    }

    /**
     * Implementation of {@link SampleMessageProvider} that generates sample Avro {@link
     * GenericRecord} messages.
     *
     * <p>The implementation offers two types of sample messages:
     *
     * <ul>
     *   <li>{@link #sampleMessage()} - Creates a complex record with nested children in a
     *       hierarchical structure
     *   <li>{@link #sampleMessageV2()} - Creates a simpler record with various Avro data types but
     *       without nested children hierarchy
     * </ul>
     *
     * <p>The generated records include various Avro data types:
     *
     * <ul>
     *   <li>Strings
     *   <li>Maps with Utf8 keys
     *   <li>Enum values
     *   <li>Fixed binary data
     *   <li>Nested records
     *   <li>Arrays (including null elements and empty arrays)
     *   <li>Null values
     * </ul>
     *
     * @implSpec This class creates structured Avro data based on the schema defined in "value.avsc"
     *     resource.
     */
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

    /**
     * Implementation of {@link SampleMessageProvider} that generates sample {@link JsonNode}
     * messages.
     *
     * <p>This provider creates a hierarchical JSON structure representing a family tree with the
     * following characteristics:
     *
     * <ul>
     *   <li>A root node named "joe"
     *   <li>Children nodes including "alex", "anna" (with her own children "gloria" and "terence"),
     *       "serena", and a null value
     *   <li>A byte array signature on the root node
     *   <li>A 2D array representing extended family members
     * </ul>
     *
     * <p>This class is useful for testing JSON serialization/deserialization and JSON data
     * processing with a complex nested structure that includes arrays, nested objects, and null
     * values.
     *
     * @implSpec The JSON structure is created using an inner class {@code Value} and converted to a
     *     JsonNode using Jackson's ObjectMapper.
     */
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

    /**
     * Implementation of {@link SampleMessageProvider} that provides sample Kafka Connect {@link
     * Struct} messages with predefined schemas and sample values.
     *
     * <p>This provider creates hierarchical data structures with nested schemas:
     *
     * <ul>
     *   <li>A root schema with fields for name, signature, children array, and nullable array
     *   <li>A children schema defining parent-child relationships
     *   <li>A grandSons schema for representing third-level nesting
     * </ul>
     *
     * <p>The sample messages demonstrate various Kafka Connect schema features including:
     *
     * <ul>
     *   <li>Nested struct types
     *   <li>Arrays of complex types
     *   <li>Optional fields (nullable)
     *   <li>Primitive types like STRING and BYTES
     *   <li>Null values in arrays
     * </ul>
     */
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
