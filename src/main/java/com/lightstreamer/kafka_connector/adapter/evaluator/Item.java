package com.lightstreamer.kafka_connector.adapter.evaluator;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lightstreamer.kafka_connector.adapter.consumers.Pair;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public class Item {

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
        ExpressionResult result = ItemExpressionEvaluator.SUBSCRIBED.eval(input);
        return new Item(itemHandle, result.prefix(),
                result.pairs().stream().map(Item::toValue).toList());
    }

    private static Value toValue(Pair<String,String> p) {
        return Value.of(p.first(), p.second());
        
    }
}
