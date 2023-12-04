package com.lightstreamer.kafka_connector.adapter.evaluator.selectors;

public interface Selector extends ValueSchema {

    String expression();

    default String expectedRoot() {
        return "";
    }

}
