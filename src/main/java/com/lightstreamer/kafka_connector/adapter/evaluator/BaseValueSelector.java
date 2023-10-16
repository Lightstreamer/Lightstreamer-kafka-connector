package com.lightstreamer.kafka_connector.adapter.evaluator;

public abstract class BaseValueSelector<V> implements ValueSelector<V> {

    private final String name;

    private final String expression;

    protected BaseValueSelector(String name, String expression) {
        this.name = name;
        this.expression = expression;
    }

    public String expression() {
        return expression;
    }

    @Override
    public String name() {
        return name;
    }
}
