package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;

public class StringBaseSelector extends BaseSelector {

    protected StringBaseSelector(String name, String expression) {
        super(name, expression);
    }

    protected String checkExpression(String expression) {
        if (expectedRoot().equals(expression)) {
            return expression;
        }

        throw new RuntimeException("Expected " + expectedRoot());
    }
}
