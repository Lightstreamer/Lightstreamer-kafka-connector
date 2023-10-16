package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Item extends BasicItem {

    private String sourceItem;

    private Structure itemStructure;

    private record Structure(String prefix, List<Value> selectors) {
    }

    Item(String sourceItem, String prefix, List<Value> selectors) {
        super(prefix, selectors);
        this.sourceItem = sourceItem;
        this.itemStructure = new Structure(prefix, selectors);
    }

    Item(String sourceItem) {
        this(sourceItem, sourceItem, Collections.emptyList());
    }

    public List<Value> values() {
        return (List<Value>) this.schemas;
    }

    public String getSourceItem() {
        return sourceItem;
    }

    public MatchResult match(Item other) {
        MatchResult result = super.match(other);
        if (!result.matched()) {
            return result;
        }

        Map<String, Value> thisMap = (Map<String, Value>) schemaMap;
        Map<String, Value> otherMap = (Map<String, Value>) other.schemaMap;

        Set<String> matchedKeys = result.matchedKeys();
        Set<String> finalMatchedKeys = new HashSet<>();
        for (String matchedKey : matchedKeys) {
            Value thisValue = thisMap.get(matchedKey);
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

    static public Item fromItem(String input) {
        int start = input.indexOf("${");
        if (start != -1) {
            int end = input.lastIndexOf("}");
            if (end != -1) {
                String prefix = input.substring(0, start);
                String expr = input.substring(start + 2, end);
                String[] terms = expr.split(",");
                List<Value> selectors = new ArrayList<>();
                for (int i = 0; i < terms.length; i++) {
                    String name = terms[i].substring(0, terms[i].indexOf('='));
                    String value = terms[i].substring(terms[i].indexOf('=') + 1);
                    selectors.add(new SimpleValue(name, value));
                }
                return new Item(input, prefix, selectors);
            }
        }
        return new Item(input);
    }
}
