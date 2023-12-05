package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser.NodeEvaluator;

abstract sealed class JsonNodeBaseSelector extends BaseSelector
        permits JsonNodeKeySelectorSupplier.JsonNodeKeySelector, JsonNodeValueSelectorSupplier.JsonNodeValueSelector {

    private static class PropertyGetter implements NodeEvaluator<JsonNode, JsonNode> {

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

    private static class ArrayGetter implements NodeEvaluator<JsonNode, JsonNode> {

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

    private final LinkedNode<NodeEvaluator<JsonNode, JsonNode>> linkedNode;

    private static final SelectorExpressionParser<JsonNode, JsonNode> PARSER = new SelectorExpressionParser.Builder<JsonNode, JsonNode>()
            .withFieldEvaluator(PropertyGetter::new)
            .withArrayEvaluator(ArrayGetter::new)
            .build();

    protected JsonNodeBaseSelector(String name, String expression) {
        super(name, expression);
        this.linkedNode = PARSER.parse(expectedRoot(), expression);
    }

    protected Value eval(JsonNode currentJsonNode) {
        LinkedNode<NodeEvaluator<JsonNode, JsonNode>> currentLinkedNode = linkedNode;
        while (currentLinkedNode != null) {
            NodeEvaluator<JsonNode, JsonNode> nodeEvaluator = currentLinkedNode.value();
            currentJsonNode = nodeEvaluator.get(currentJsonNode);
            currentLinkedNode = currentLinkedNode.next();
        }
        return new SimpleValue(name(), currentJsonNode.asText());
    }
}
