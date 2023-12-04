package com.lightstreamer.kafka_connector.adapter.evaluator.selectors;

public interface Value extends ValueSchema {

    default boolean isContainer() {
        return false;
    }

    static Value of(String name, String text) {
        return new SimpleValue(name, text);
    }

    default boolean match(Value other) {
        return text().equals(other.text());
    }

    String text();
}
