package com.lightstreamer.kafka_connector.adapter.evaluator;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public class Item {

    private static final Pattern ITEM = Pattern.compile("([a-zA-Z0-9_-]+)(-<(.*)>)?");

    private static final Pattern QUERY_PARAMS = Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?");

    private final Object itemHandle;

    private final Structure itemStructure;

    private final BasicItem core;

    private final Map<String, Value> valuesMap;

    private record Structure(String prefix, List<Value> selectors) {
    }

    Item(Object itemHandle, String prefix, List<Value> values) {
        this.valuesMap = values.stream().collect(toMap(Value::name, identity()));
        this.core = new BasicItem(prefix, new HashSet<>(values.stream().map(Value::name).toList()));
        this.itemHandle = itemHandle;
        this.itemStructure = new Structure(prefix, values);
    }

    Item(String sourceItem) {
        this(sourceItem, sourceItem, emptyList());
    }

    public BasicItem core() {
        return this.core;
    }

    public List<Value> values() {
        return List.copyOf(valuesMap.values());
    }

    public Object getItemHandle() {
        return itemHandle;
    }

    public MatchResult match(Item other) {
        MatchResult result = core.matchStructure(other.core);
        if (!result.matched()) {
            return result;
        }

        Map<String, Value> otherMap = other.valuesMap;

        Set<String> matchedKeys = result.matchedKeys();
        Set<String> finalMatchedKeys = new HashSet<>();
        for (String matchedKey : matchedKeys) {
            Value thisValue = valuesMap.get(matchedKey);
            Value otherValue = otherMap.get(matchedKey);
            if (thisValue.match(otherValue)) {
                finalMatchedKeys.add(matchedKey);
            }
        }

        return new MatchResult(finalMatchedKeys, matchedKeys.equals(finalMatchedKeys));
    }

    public String toString() {
        return itemStructure.toString();
    }

    static public Item of(String input, Object itemHandle) {
        Matcher matcher = ITEM.matcher(input);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid item");
        }
        List<Value> queryParams = new ArrayList<>();
        String prefix = matcher.group(1);
        String queries = matcher.group(3);
        if (queries != null) {
            Matcher m = QUERY_PARAMS.matcher(queries);
            int previousEnd = 0;
            while (m.find()) {
                if (m.start() != previousEnd) {
                    break;
                }
                String name = m.group(2);
                String value = m.group(3);
                queryParams.add(Value.of(name, value));
                previousEnd = m.end();
            }
            if (previousEnd < queries.length()) {
                throw new RuntimeException("Invalid query parameter");
            }
        }
        return new Item(itemHandle, prefix, queryParams);
    }
}
