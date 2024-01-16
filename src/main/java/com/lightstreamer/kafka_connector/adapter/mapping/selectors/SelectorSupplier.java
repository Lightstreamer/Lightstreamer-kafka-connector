package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public interface SelectorSupplier<S extends Selector> {

    S newSelector(String name, String expression);

    default void config(ConnectorConfig configuration) {

    }

    default String deserializer(ConnectorConfig config) {
        return StringDeserializer.class.getName();
    }

    default boolean maySupply(String expression) {
        return expression.startsWith(expectedRoot() + ".");
    }

    default String expectedRoot() {
        return "";
    }
}
