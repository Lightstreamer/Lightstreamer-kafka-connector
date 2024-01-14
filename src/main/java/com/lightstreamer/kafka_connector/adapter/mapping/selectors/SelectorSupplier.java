package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;
import java.util.Properties;

public interface SelectorSupplier<S extends Selector> {

    S newSelector(String name, String expression);

    default void config(Map<String, String> configuration, Properties props) {
        
    }

    default boolean maySupply(String expression) {
        return expression.startsWith(expectedRoot() + ".");
    }

    default String expectedRoot() {
        return "";
    }
}
