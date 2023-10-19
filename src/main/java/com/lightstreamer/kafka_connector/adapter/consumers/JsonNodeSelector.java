package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.BaseValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

interface NodeEvaluator {
    JsonNode get(JsonNode node);
}

record NodeValue(String name, String text) implements Value {
}

class PropertyGetter implements NodeEvaluator {

    private String name;

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

class ArrayGetter implements NodeEvaluator {

    private static Pattern pattern = Pattern.compile("\\[(\\d*)\\]");

    private String name;

    private final PropertyGetter getter;

    private final List<Integer> indexes;

    public ArrayGetter(String fieldName, int lbracket) {
        this.name = fieldName.substring(0, lbracket);
        this.getter = new PropertyGetter(name);
        this.indexes = parse(fieldName.substring(lbracket));
    }

    private static List<Integer> parse(String indexedExpression) {
        List<Integer> indexes = new ArrayList<>();
        Matcher matcher = pattern.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                String invalidTerm = indexedExpression.substring(previousEnd, currentStart);
                throw new RuntimeException("No valid expression: " + invalidTerm);
            }
            previousEnd = matcher.end();
            String group = matcher.group(1);
            if (group.length() > 0) {
                indexes.add(Integer.parseInt(matcher.group(1)));
            } else {
                indexes.add(-1); // Splat expression
            }
        }
        return indexes;
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

public class JsonNodeSelector extends BaseValueSelector<String, JsonNode> {

    private List<NodeEvaluator> evaluatorsList;

    public JsonNodeSelector(String name, String expression) {
        super(name, expression);
        System.out.println("Creating navigator for " + expression);
        this.evaluatorsList = evaluators(expression);
    }

    List<NodeEvaluator> evaluators(String stmt) {
        List<NodeEvaluator> fieldEvaluators = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(stmt, ".");
        while (st.hasMoreTokens()) {
            String fieldName = st.nextToken();
            int lbracket = fieldName.indexOf('[');
            if (lbracket != -1) {
                fieldEvaluators.add(new ArrayGetter(fieldName, lbracket));
            } else {
                fieldEvaluators.add(new PropertyGetter(fieldName));
            }
        }
        return fieldEvaluators;
    }

    @Override
    public Value extract(ConsumerRecord<String, JsonNode> node) {
        JsonNode currentNode = node.value();
        Iterator<NodeEvaluator> iterator = evaluatorsList.iterator();

        while (iterator.hasNext()) {
            NodeEvaluator evaluator = iterator.next();
            currentNode = evaluator.get(currentNode);
            if (currentNode.isValueNode()) {
                break;
            }
        }
        return new NodeValue("", currentNode.asText());
    }

    public static void main(String[] args) {
        new JsonNodeSelector("name", "expression");
    }
}
