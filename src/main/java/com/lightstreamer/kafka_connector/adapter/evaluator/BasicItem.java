package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class BasicItem {

    public record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    private final String prefix;

    protected Map<String, ? extends ValueSchema> schemaMap;

    protected List<? extends ValueSchema> schemas;

    public BasicItem(String prefix, List<? extends ValueSchema> schemas) {
        this.prefix = prefix;
        this.schemas = schemas;
        this.schemaMap = schemas.stream().collect(Collectors.toMap(ValueSchema::name, Function.identity()));
    }

    public String prefix() {
        return prefix;
    }

    public List<? extends ValueSchema> schemas() {
        return Collections.unmodifiableList(schemas);
    }

    public MatchResult match(BasicItem other) {
        if (!other.prefix.equals(prefix)) {
            return new MatchResult(Collections.emptySet(), false);
        }

        Set<String> thisKeys = schemaMap.keySet();
        Set<String> otherKeys = other.schemaMap.keySet();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }
}
