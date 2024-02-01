/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

public interface Value {

  String name();

  String text();

  static Value of(String name, String text) {
    return new SimpleValue(name, text);
  }
}

record SimpleValue(String name, String text) implements Value {}
