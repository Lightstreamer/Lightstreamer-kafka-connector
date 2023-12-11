package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public interface ItemSchema {

    public record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    public String prefix();

    public Set<String> keys();

    default public MatchResult matches(ItemSchema other) {
        if (!other.prefix().equals(prefix())) {
            return new MatchResult(Collections.emptySet(), false);
        }

        Set<String> thisKeys = keys();
        Set<String> otherKeys = other.keys();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }

    static ItemSchema of(String prefix, Set<String> keys) {
        return new DefaultItemSchema(prefix, keys);
    }

    static ItemSchema of(String prefix, Set<Value> values) {
        return of(prefix, values.stream().map(Value::name).collect(Collectors.toSet()));
    }
}

record DefaultItemSchema(
        String prefix, Set<String> keys) implements ItemSchema {
}
