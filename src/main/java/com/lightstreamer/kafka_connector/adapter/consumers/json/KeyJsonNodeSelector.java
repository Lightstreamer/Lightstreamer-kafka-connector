package com.lightstreamer.kafka_connector.adapter.consumers.json;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public final class KeyJsonNodeSelector extends BaseJsonNodeSelector implements KeySelector<JsonNode> {

    protected KeyJsonNodeSelector(String name, String expression) {
        super(name, expression);
    }

    @Override
    public Value extract(ConsumerRecord<JsonNode, ?> record) {
        return super.eval(record.key());
    }
}
