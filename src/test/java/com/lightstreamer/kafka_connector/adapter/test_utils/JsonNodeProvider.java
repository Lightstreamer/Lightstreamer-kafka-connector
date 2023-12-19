package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonNodeProvider {

    private JsonNodeProvider() {
    }

    private static JsonNodeProvider PROVIDER = new JsonNodeProvider();

    public static JsonNode RECORD = PROVIDER.newGenericRecord();

    private ObjectNode newGenericRecord() {
        Value value = new Value("joe",
                List.of(new Value("alex"),
                        new Value("anna",
                                List.of(new Value("gloria"), new Value("terence"))),
                        new Value("serena")));

        ObjectNode node = new ObjectMapper().valueToTree(value);
        return node;
    }
}

class Value {

    public String name;

    public Value child;

    public List<Value> children;

    public Value[][] family;

    public Value(String name) {
        this.name = name;
    }

    public Value(String name, List<Value> children) {
        this(name);
        this.children = List.copyOf(children);
    }

    public Value(String name, Value[][] family) {
        this(name);
        this.family = family;
    }
}