package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface Schema {

    record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    public interface SchemaName {

        String id();

        static SchemaName of(String id) {
            return new DefaultSchemaName(id);
        }

    }

    Set<String> keys();

    SchemaName name();

    default boolean isEmpty() {
        return keys().isEmpty();
    }

    default public MatchResult matches(Schema other) {
        if (!name().equals(other.name())) {
            return new MatchResult(Collections.emptySet(), false);
        }
        Set<String> thisKeys = keys();
        Set<String> otherKeys = other.keys();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }

    static Schema of(SchemaName name, Set<String> keys) {
        return new DefaultSchema(name, keys);
    }

    static Schema of(SchemaName name, String k1) {
        return new DefaultSchema(name, Set.of(k1));
    }

    static Schema of(SchemaName name, String k1, String k2) {
        return new DefaultSchema(name, Set.of(k1, k2));
    }

    static Schema of(SchemaName name, String k1, String k2, String k3) {
        return new DefaultSchema(name, Set.of(k1, k2, k3));
    }

    static Schema empty(SchemaName name) {
        return new DefaultSchema(name, Collections.emptySet());
    }

    static Schema empty(String name) {
        return empty(SchemaName.of(name));
    }

}

record DefaultSchema(SchemaName name, Set<String> keys) implements Schema {
}

record DefaultSchemaName(String id) implements Schema.SchemaName {

}