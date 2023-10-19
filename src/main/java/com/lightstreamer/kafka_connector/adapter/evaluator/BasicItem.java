package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BasicItem {

    public record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    private final String prefix;

    private Set<String> keys;

    BasicItem(String prefix, Set<String> keys) {
        this.prefix = prefix;
        this.keys = Collections.unmodifiableSet(keys);
    }

    public String prefix() {
        return prefix;
    }

    public Set<String> keys() {
        return this.keys;
    }

    protected MatchResult matchStructure(BasicItem other) {
        if (!other.prefix.equals(prefix)) {
            return new MatchResult(Collections.emptySet(), false);
        }

        Set<String> thisKeys = this.keys;
        Set<String> otherKeys = other.keys;

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }
}
