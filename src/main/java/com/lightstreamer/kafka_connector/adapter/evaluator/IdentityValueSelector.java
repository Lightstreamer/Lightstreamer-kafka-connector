package com.lightstreamer.kafka_connector.adapter.evaluator;

public class IdentityValueSelector extends BaseSelector {

    public IdentityValueSelector(String name, String expr) {
        super(name, expr);
    }

    public Value extract(String t) {
        return new SimpleValue(name(), t.transform(s -> "extracted <%s>".formatted(s)));
    }

}
