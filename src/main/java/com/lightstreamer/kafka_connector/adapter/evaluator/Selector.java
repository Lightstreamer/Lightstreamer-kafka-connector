package com.lightstreamer.kafka_connector.adapter.evaluator;

public interface Selector extends ValueSchema {

    String expression();

    default String expectedRoot() {
        return "";
    }

}
