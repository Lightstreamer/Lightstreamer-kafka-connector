package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.lightstreamer.kafka_connector.adapter.consumers.avro.GenericRecordSelectorTest;

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

        parentJoe.put("children", new GenericData.Array<>(childrenSchema, List.of(childAlex, childAnna, childSerena)));
        return parentJoe;
    }
}
