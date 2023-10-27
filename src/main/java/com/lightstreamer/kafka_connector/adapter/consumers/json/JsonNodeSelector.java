package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

class PropertyGetter implements NodeEvaluator<JsonNode, JsonNode> {

    private final String name;

    public PropertyGetter(String name) {
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

    public ArrayGetter(String fieldName, List<Integer> indexes) {
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

    private List<NodeEvaluator<JsonNode, JsonNode>> evaluatorsList;

    private static final ExpressionParser<JsonNode, JsonNode> PARSER = new ExpressionParser.Builder<JsonNode, JsonNode>()
            .withFieldEvaluator(PropertyGetter::new)
            .withArrayEvaluator(ArrayGetter::new)
            .build();

    public JsonNodeSelector(String name, String expression) {
        super(name, expression);
        this.evaluatorsList = PARSER.parse(expression);
    }

    // List<NodeEvaluator> evaluators(String expression) {
    // List<NodeEvaluator> fieldEvaluators = new ArrayList<>();
    // StringTokenizer st = new StringTokenizer(expression, ".");
    // if (st.hasMoreTokens()) {
    // String nextToken = st.nextToken();
    // // NodeEvaluator root = switch (nextToken) {
    // // case "VALUE" -> (node, record) -> record.value();
    // // }
    // // fieldEvaluators.add(new RootGetter(nextToken));
    // }
    // while (st.hasMoreTokens()) {
    // String fieldName = st.nextToken();
    // int lbracket = fieldName.indexOf('[');
    // if (lbracket != -1) {
    // fieldEvaluators.add(new ArrayGetter(fieldName, lbracket));
    // } else {
    // fieldEvaluators.add(new PropertyGetter(fieldName));
    // }
    // }
    // return fieldEvaluators;
    // }

    @Override
    public Value extract(JsonNode value) {
        Iterator<NodeEvaluator<JsonNode, JsonNode>> iterator = evaluatorsList.iterator();
        JsonNode currentNode = value;
        while (iterator.hasNext()) {
            NodeEvaluator<JsonNode, JsonNode> evaluator = iterator.next();
            currentNode = evaluator.get(currentNode);
            if (currentNode.isValueNode()) {
                break;
            }
        }
        return new SimpleValue(name(), currentNode.asText());
    }

    // static ValueSelectorSupplier<?> make() {
    // return JsonNodeSelector::new;
    // }

}
