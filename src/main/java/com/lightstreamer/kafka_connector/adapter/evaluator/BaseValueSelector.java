package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Objects;

public abstract class BaseValueSelector<K, V> implements ValueSelector<K, V> {

    private final String name;

    private final String expression;

    protected BaseValueSelector(String name, String expression) {
        this.name = Objects.requireNonNull(name);
        this.expression = Objects.requireNonNull(expression);
    }

    public String expression() {
        return expression;
    }

    @Override
    public String name() {
        return name;
    }
}
