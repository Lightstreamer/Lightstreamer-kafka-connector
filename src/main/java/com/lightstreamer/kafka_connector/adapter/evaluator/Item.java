package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemSchema.MatchResult;

public class Item {

    private final Object itemHandle;

    private final Map<String, String> valuesMap;

    private final ItemSchema schema;

    Item(Object itemHandle, String prefix, Map<String, String> values) {
        this.valuesMap = values;
        this.itemHandle = itemHandle;
        this.schema = ItemSchema.of(prefix, values.keySet());
    }

    public ItemSchema schema() {
        return schema;
    }

    Map<String, String> values() {
        return Map.copyOf(valuesMap);
    }

    public Object itemHandle() {
        return itemHandle;
    }

    public boolean matches(Item other) {
        MatchResult result = schema.matches(other.schema());
        if (!result.matched()) {
            return false;
        }

        return result.matchedKeys()
                .stream()
                .allMatch(key -> valuesMap.get(key).equals(other.valuesMap.get(key)));
    }

    static public Item of(String input, Object itemHandle) throws EvaluationException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        return new Item(itemHandle, result.prefix(), result.params());
    }
}
