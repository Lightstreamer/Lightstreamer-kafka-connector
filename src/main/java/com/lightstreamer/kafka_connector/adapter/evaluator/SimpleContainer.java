package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.List;

public class SimpleContainer implements ValueContainer {

    private String name; 

    private final List<? extends Value> values;

    public SimpleContainer(String name, List<? extends Value> values) {
        this.name = name;
        this.values = List.copyOf(values);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String text() {
        return "";
    }

    @Override
    public List<? extends Value> values() {
        return values;
    }
    
}
