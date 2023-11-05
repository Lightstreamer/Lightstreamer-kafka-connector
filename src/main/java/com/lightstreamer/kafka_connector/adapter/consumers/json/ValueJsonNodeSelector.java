package com.lightstreamer.kafka_connector.adapter.consumers.json;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;

public final class ValueJsonNodeSelector extends BaseJsonNodeSelector implements ValueSelector<JsonNode> {

    protected ValueJsonNodeSelector(String name, String expression) {
        super(name, expression);
    }

    @Override
    public Value extract(ConsumerRecord<?, JsonNode> record) {
        return super.eval(record.value());
    }
}
