package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.List;

public interface ValueContainer extends Value {

    @Override
    default boolean isContainer() {
        return true;
    }

    @Override
    default boolean match(Value other) {
        if (!other.isContainer()) {
            return values().contains(other);
        }
        return values().equals(((ValueContainer) other).values());
    }

    List<? extends Value> values();

    static ValueContainer of(String name, Value v1) {
        return new SimpleContainer(name, List.of(v1));
    }

    static ValueContainer of(String name, Value v1, Value v2) {
        return new SimpleContainer(name, List.of(v1, v2));
    }

    static ValueContainer of(String name, Value v1, Value v2, Value v3) {
        return new SimpleContainer(name, List.of(v1, v2, v3));
    }

    static ValueContainer of(String name, Value v1, Value v2, Value v3, Value v4) {
        return new SimpleContainer(name, List.of(v1, v2, v3, v4));
    }
}