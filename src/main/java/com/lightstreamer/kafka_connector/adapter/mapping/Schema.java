package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface Schema {

    record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    Set<String> keys();

    String tag();

    default boolean isEmpty() {
        return keys().isEmpty();
    }

    default public MatchResult matches(Schema other) {
        Set<String> thisKeys = keys();
        Set<String> otherKeys = other.keys();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }

    static Schema of(String tag, Set<String> keys) {
        return new DefaultSchema(tag, keys);
    }

    static Schema of(String tag, String k1) {
        return new DefaultSchema(tag, Set.of(k1));
    }

    static Schema of(String tag, String k1, String k2) {
        return new DefaultSchema(tag, Set.of(k1, k2));
    }

    static Schema of(String tag, String k1, String k2, String k3) {
        return new DefaultSchema(tag, Set.of(k1, k2, k3));
    }

    static Schema empty(String tag) {
        return new DefaultSchema(tag, Collections.emptySet());
    }

}

record DefaultSchema(String tag, Set<String> keys) implements Schema {
}
