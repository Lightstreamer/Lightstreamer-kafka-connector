package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Set;

public interface ValuesContainer {
    
    Selectors<?,?> selectors();

    Set<Value> values();

    static ValuesContainer of(Selectors<?,?> selectors, Set<Value> values) {
        return new DefaultValuesContainer(selectors, values);
    }

}

record DefaultValuesContainer(Selectors<?,?> selectors, Set<Value> values) implements ValuesContainer {

}