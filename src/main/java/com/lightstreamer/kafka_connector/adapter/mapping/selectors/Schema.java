/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface Schema {

  record MatchResult(Set<String> matchedKeys, boolean matched) {}

  Set<String> keys();

  String name();

  default boolean isEmpty() {
    return keys().isEmpty();
  }

  public default MatchResult matches(Schema other) {
    if (!name().equals(other.name())) {
      return new MatchResult(Collections.emptySet(), false);
    }
    Set<String> thisKeys = keys();
    Set<String> otherKeys = other.keys();

    HashSet<String> matchedKeys = new HashSet<>(thisKeys);
    matchedKeys.retainAll(otherKeys);

    return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
  }

  static Schema from(String name, Set<String> keys) {
    return new DefaultSchema(name, keys);
  }

  static Schema empty(String name) {
    return new DefaultSchema(name, Collections.emptySet());
  }
}

record DefaultSchema(String name, Set<String> keys) implements Schema {}
