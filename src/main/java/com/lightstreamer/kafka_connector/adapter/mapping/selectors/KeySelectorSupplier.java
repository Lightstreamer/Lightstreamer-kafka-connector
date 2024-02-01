/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.common.serialization.Deserializer;

public interface KeySelectorSupplier<V> extends SelectorSupplier<KeySelector<V>> {

  KeySelector<V> newSelector(String name, String expression);

  Deserializer<V> deseralizer();

  default String expectedRoot() {
    return "KEY";
  }
}
