package com.lightstreamer.kafka_connector.adapter.consumers.json;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

abstract sealed class BaseJsonNodeSelector extends BaseSelector permits ValueJsonNodeSelector, KeyJsonNodeSelector {

    private final LinkedNode<NodeEvaluator<JsonNode, JsonNode>> linkedNode;

    private static final ExpressionParser<JsonNode, JsonNode> PARSER = new ExpressionParser.Builder<JsonNode, JsonNode>()
            .withFieldEvaluator(PropertyGetter::new)
            .withArrayEvaluator(ArrayGetter::new)
            .build();

    protected BaseJsonNodeSelector(String name, String expression) {
        super(name, expression);
        this.linkedNode = PARSER.parse(expression);
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
