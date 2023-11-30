package com.lightstreamer.kafka_connector.adapter.evaluator;

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
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BaseSelector other = (BaseSelector) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (expression == null) {
            if (other.expression != null)
                return false;
        } else if (!expression.equals(other.expression))
            return false;
        return true;
    }
}
