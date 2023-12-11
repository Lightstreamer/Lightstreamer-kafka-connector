package com.lightstreamer.kafka_connector.adapter.evaluator;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.lightstreamer.kafka_connector.adapter.consumers.Pair;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemSchema.MatchResult;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public class Item {

    private final Object itemHandle;

    private final Structure itemStructure;

    private final ItemSchema schema;

    private final Map<String, Value> valuesMap;

    private record Structure(String prefix, List<Value> selectors) {
    }

    Item(Object itemHandle, String prefix, List<Value> values) {
        this.valuesMap = values.stream().collect(toMap(Value::name, identity()));
        this.schema = ItemSchema.of(prefix, values);
        this.itemHandle = itemHandle;
        this.itemStructure = new Structure(prefix, values);
    }

    Item(Object itemHandle, ItemSchema schema, List<Value> values) {
        this.valuesMap = values.stream().collect(toMap(Value::name, identity()));
        this.schema = schema;
        this.itemHandle = itemHandle;
        this.itemStructure = new Structure(schema.prefix(), values);
    }

    public ItemSchema schema() {
        return this.schema;
    }

    List<Value> values() {
        return List.copyOf(valuesMap.values());
    }

    public Object getItemHandle() {
        return itemHandle;
    }

    public boolean matches(Item other) {
        MatchResult result = schema.matches(other.schema());
        if (!result.matched()) {
            return false;
        }

        return result.matchedKeys()
                .stream()
                .allMatch(key -> matches(key, valuesMap, other.valuesMap));
    }

    private static boolean matches(String key, Map<String, Value> m1, Map<String, Value> m2) {
        return m1.get(key).matches(m2.get(key));
    }

    public String toString() {
        return itemStructure.toString();
    }

    static public Item of(String input, Object itemHandle) throws EvaluationException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        Set<Value> values = result.pairs()
                .stream()
                .map(Item::toValue)
                .collect(Collectors.toSet());
        return new Item(itemHandle, ItemSchema.of(result.prefix(), values), values);
    }

    private static Value toValue(Pair<String, String> p) {
        return Value.of(p.first(), p.second());
    }
}
