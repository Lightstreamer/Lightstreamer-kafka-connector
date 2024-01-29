package com.lightstreamer.kafka_connector.adapter.test_utils;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonNodeProvider {

    private JsonNodeProvider() {
    }

    public static JsonNode RECORD = new JsonNodeProvider().newNode();

    private ObjectNode newNode() {
        List<Value> parentJoeChildren = new ArrayList<>(
                List.of(new Value("alex"),
                        new Value("anna",
                                List.of(new Value("gloria"), new Value("terence"))),
                        new Value("serena")));
        parentJoeChildren.add(null);

        Value value = new Value("joe", parentJoeChildren);
        value.signature = new byte[]{97, 98, 99, 100};

        ObjectNode node = new ObjectMapper().valueToTree(value);
        return node;
    }
}

class Value {

    public String name;

    public Value child;

    public List<Value> children;

    public byte[] signature;

    public Value[][] family;

    public Value(String name) {
        this.name = name;
    }

    public Value(String name, List<Value> children) {
        this(name);
        this.children = children;
    }

    public Value(String name, Value[][] family) {
        this(name);
        this.family = family;
    }
}