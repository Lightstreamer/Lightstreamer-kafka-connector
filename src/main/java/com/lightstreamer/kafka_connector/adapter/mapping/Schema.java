package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.HashSet;
import java.util.Set;

public interface Schema {

    record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    public Set<String> keys();

    default public MatchResult matches(Schema other) {
        Set<String> thisKeys = keys();
        Set<String> otherKeys = other.keys();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }

    static Schema of(Set<String> keys) {
        return new DefaultItemSchema(keys);
    }

}

record DefaultItemSchema(Set<String> keys) implements Schema {
}
