package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapter.mapping.Schema.MatchResult;

public class Item {

    private final Object itemHandle;

    private final Map<String, String> valuesMap;

    private final Schema schema;

    private final String prefix;

    Item(Object itemHandle, String prefix, Map<String, String> values) {
        this.valuesMap = values;
        this.prefix = prefix;
        this.itemHandle = itemHandle;
        this.schema = Schema.of(values.keySet());
    }

    String prefix() {
        return prefix;
    }

    Schema schema() {
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
