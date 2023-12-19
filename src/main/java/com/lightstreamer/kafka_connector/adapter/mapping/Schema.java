package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface Schema {

    record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    public Set<String> keys();

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

    static Schema of(Set<String> keys) {
        return new DefaultSchema(keys);
    }

    static Schema of(String k1) {
        return new DefaultSchema(Set.of(k1));
    }

    static Schema of(String k1, String k2) {
        return new DefaultSchema(Set.of(k1, k2));
    }

    static Schema of(String k1, String k2, String k3) {
        return new DefaultSchema(Set.of(k1, k2, k3));
    }

    static Schema empty() {
        return new DefaultSchema(Collections.emptySet());
    }

}

record DefaultSchema(Set<String> keys) implements Schema {
}
