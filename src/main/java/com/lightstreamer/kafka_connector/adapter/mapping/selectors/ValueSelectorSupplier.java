/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.common.serialization.Deserializer;

public interface ValueSelectorSupplier<V> extends SelectorSupplier<ValueSelector<V>> {

  ValueSelector<V> newSelector(String name, String expression);

  Deserializer<V> deseralizer();

  default String expectedRoot() {
    return "VALUE";
  }
}
