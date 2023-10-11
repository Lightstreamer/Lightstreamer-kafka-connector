package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordEvaluator;

interface NodeEvaluator {
    JsonNode get(JsonNode node);
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

    private static Pattern pattern = Pattern.compile("\\[(\\d+)\\]");

    private String name;

    private final PropertyGetter getter;

    private final List<Integer> indexes;

    public ArrayGetter(String fieldName, int lbracket) {
        this.name = fieldName.substring(0, lbracket);
        this.getter = new PropertyGetter(name);
        this.indexes = parse(fieldName.substring(lbracket));
    }

    private static List<Integer> parse(String indexedExpression) {
        List<Integer> index = new ArrayList<>();
        Matcher matcher = pattern.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                String invalidTerm = indexedExpression.substring(previousEnd, currentStart);
                throw new RuntimeException("No valid expression: " + invalidTerm);
            }
            previousEnd = matcher.end();
            index.add(Integer.parseInt(matcher.group(1)));
        }
        return index;
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

public class JsonNodeEvaluator implements RecordEvaluator<JsonNode> {

    private String statement;

    private List<NodeEvaluator> evaluatorsList;

    public JsonNodeEvaluator(String statement) {
        System.out.println("Creating navigator for " + statement);
        Objects.requireNonNull(statement);
        this.statement = statement;
        this.evaluatorsList = evaluators(statement);
    }

    List<NodeEvaluator> evaluators(String stmt) {
        List<NodeEvaluator> fieldEvaluators = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(stmt, ".");
        // if (st.hasMoreTokens()) {
        // fieldEvaluators.add(new RootGetter(st.nextToken()));
        // }

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
    public String extract(JsonNode node) {
        JsonNode currentNode = node;
        Iterator<NodeEvaluator> iterator = evaluatorsList.iterator();

        while (iterator.hasNext()) {
            NodeEvaluator evaluator = iterator.next();
            currentNode = evaluator.get(currentNode);
            if (currentNode.isValueNode()) {
                break;
            }
        }
        return currentNode.asText();
    }
}
