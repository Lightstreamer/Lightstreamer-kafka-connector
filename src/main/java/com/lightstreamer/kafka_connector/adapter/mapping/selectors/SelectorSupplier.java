/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

public interface SelectorSupplier<S extends Selector> {

  S newSelector(String name, String expression);

  default boolean maySupply(String expression) {
    return expression.startsWith(expectedRoot() + ".");
  }

  default String expectedRoot() {
    return "";
  }
}
