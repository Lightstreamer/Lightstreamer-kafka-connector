package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;

public interface Value extends ValueSchema {

    String text();

    default boolean isContainer() {
        return false;
    }

    static Value of(String name, String text) {
        return new SimpleValue(name, text);
    }

    static Value of(Map.Entry<String, String> entry) {
        return of(entry.getKey(), entry.getValue());
    }

}

record SimpleValue(String name, String text) implements Value {
}
