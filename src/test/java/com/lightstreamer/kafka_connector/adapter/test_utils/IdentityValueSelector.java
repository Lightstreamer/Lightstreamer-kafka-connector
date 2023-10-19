package com.lightstreamer.kafka_connector.adapter.test_utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public class IdentityValueSelector extends BaseValueSelector<String, String> {

    public IdentityValueSelector(String name, String expr) {
        super(name, expr);
    }

    @Override
    public Value extract(ConsumerRecord<String, String> t) {
        return new SimpleValue(name(), t.value().transform(s -> "extracted <%s>".formatted(s)));
    }

}
