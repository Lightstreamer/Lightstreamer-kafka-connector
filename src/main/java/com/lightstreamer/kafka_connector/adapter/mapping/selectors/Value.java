package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.SchemaName;

public interface Value extends ValueSchema {

    SchemaName schemaName();

    String text();

    default boolean isContainer() {
        return false;
    }

    static Value of(SchemaName schemaName, String name, String text) {
        return new SimpleValue(schemaName, name, text);
    }

    static Value of(SchemaName schemaName, Map.Entry<String, String> entry) {
        return of(schemaName, entry.getKey(), entry.getValue());
    }

}

record SimpleValue(SchemaName schemaName, String name, String text) implements Value {
}
