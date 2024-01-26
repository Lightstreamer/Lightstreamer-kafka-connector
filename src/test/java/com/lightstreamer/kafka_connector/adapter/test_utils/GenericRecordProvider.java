package com.lightstreamer.kafka_connector.adapter.test_utils;

import static org.junit.jupiter.api.DynamicTest.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorTest;

public class GenericRecordProvider {

    private final Schema valueSchema;

    private final Schema childrenSchema;

    private GenericRecordProvider() {
        ClassLoader classLoader = GenericRecordSelectorTest.class.getClassLoader();
        Schema.Parser parser = new Schema.Parser();

        try {
            valueSchema = parser.parse(classLoader.getResourceAsStream("value.avsc"));
            childrenSchema = parser.parse(classLoader.getResourceAsStream("array.avsc"));
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    private static GenericRecordProvider PROVIDER = new GenericRecordProvider();

    public static GenericRecord RECORD = PROVIDER.newGenericRecord();

    private GenericRecord newGenericRecord() {
        GenericRecord parentJoe = new GenericData.Record(valueSchema);
        parentJoe.put("name", "joe");
        parentJoe.put("preferences", Map.of(new Utf8("pref1"), "pref_value1", new Utf8("pref2"), "pref_value2"));

        // GenericRecord documentRecord = new GenericData.Record(valueSchema);
        // documentRecord.put("doc_id", "ID123");
        // documentRecord.put("doc_type", "ID");
        // parentJoe.put("documents", Map.of("id", documentRecord));

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
        childAnna.put("children", new GenericData.Array<>(childrenSchema, List.of(childGloria, childTerence)));

        ArrayList<GenericRecord> joeChildren = new ArrayList<>(List.of(childAlex, childAnna, childSerena));
        joeChildren.add(null);
        parentJoe.put("children", new GenericData.Array<>(childrenSchema, joeChildren));
        return parentJoe;
    }

    public static void main(String[] args) {
        System.out.println(RECORD);

    }
}
