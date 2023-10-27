package com.lightstreamer.kafka_connector.adapter.consumers.raw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public class RawValueSelector extends BaseSelector<String> {

    protected static Logger log = LoggerFactory.getLogger(RawValueSelector.class);

    public RawValueSelector(String name, String expression) {
        super(name, expression);
    }

    public RawValueSelector(String name) {
        super(name, "NO_EXPRESSION");
    }

    @Override
    public Value extract(String object) {
        return Value.of(name(), object);
    }
}
