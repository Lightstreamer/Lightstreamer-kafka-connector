package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Objects;

public abstract class BaseSelector implements Selector {

    private final String name;

    private final String expression;

    protected BaseSelector(String name, String expression) {
        this.name = checkName(name);
        this.expression = checkExpression(expression);
    }

    protected String checkName(String name) {
        return Objects.requireNonNull(name);
    }

    protected String checkExpression(String expression) {
        return Objects.requireNonNull(expression);
    }

    @Override
    public String expression() {
        return expression;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        return obj instanceof BaseSelector other &&
                Objects.equals(name, other.name) &&
                Objects.equals(expression, other.expression);
    }
}
