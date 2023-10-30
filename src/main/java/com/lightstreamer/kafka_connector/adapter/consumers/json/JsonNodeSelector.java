package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

class PropertyGetter implements NodeEvaluator<JsonNode, JsonNode> {

    private final String name;

    PropertyGetter(String name) {
        this.name = name;
    }

    @Override
    public JsonNode get(JsonNode node) {
        if (!node.has(name)) {
            throw new RuntimeException("No field <" + name + "> exists!");
        }
        return node.get(name);
    }
}

class ArrayGetter implements NodeEvaluator<JsonNode, JsonNode> {

    private final String name;

    private final PropertyGetter getter;

    private final List<Integer> indexes;

    ArrayGetter(String fieldName, List<Integer> indexes) {
        this.name = fieldName;
        this.indexes = indexes;
        this.getter = new PropertyGetter(name);
    }

    @Override
    public JsonNode get(JsonNode node) {
        JsonNode value = getter.get(node);
        for (int i : indexes) {
            if (value.isArray()) {
                value = value.get(i);
            } else {
                throw new RuntimeException("Current evaluated field is not an Array");
            }
        }
        return value;
    }
}

public class JsonNodeSelector extends BaseSelector<JsonNode> {

    private final LinkedNode<NodeEvaluator<JsonNode, JsonNode>> linkedNode;

    private static final ExpressionParser<JsonNode, JsonNode> PARSER = new ExpressionParser.Builder<JsonNode, JsonNode>()
            .withFieldEvaluator(PropertyGetter::new)
            .withArrayEvaluator(ArrayGetter::new)
            .build();

    public JsonNodeSelector(String name, String expression) {
        super(name, expression);
        this.linkedNode = PARSER.parse(expression);
    }

    @Override
    public Value extract(JsonNode value) {
        LinkedNode<NodeEvaluator<JsonNode, JsonNode>> currentLinkedNode = linkedNode;
        JsonNode currentJsonNode = value;
        while (currentLinkedNode != null) {
            NodeEvaluator<JsonNode, JsonNode> nodeEvaluator = currentLinkedNode.value();
            currentJsonNode = nodeEvaluator.get(currentJsonNode);
            currentLinkedNode = currentLinkedNode.next();
        }
        return new SimpleValue(name(), currentJsonNode.asText());
    }
}
