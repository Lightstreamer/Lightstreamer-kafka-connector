package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;

public interface Value extends ValueSchema {

    String tag();

    String text();

    default boolean isContainer() {
        return false;
    }

    static Value of(String tag, String name, String text) {
        return new SimpleValue(tag, name, text);
    }

    static Value of(String tag, Map.Entry<String, String> entry) {
        return of(tag, entry.getKey(), entry.getValue());
    }

}

record SimpleValue(String tag, String name, String text) implements Value {
}
