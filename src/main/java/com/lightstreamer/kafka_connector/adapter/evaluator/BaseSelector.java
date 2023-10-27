package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Objects;

public abstract class BaseSelector<V> implements Selector<V> {

    private final String name;

    private final String expression;

    protected BaseSelector(String name, String expression) {
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
