package com.lightstreamer.kafka_connector.adapter.evaluator;

public interface Selector<V> extends ValueSchema {

    String expression();

    Value extract(V object);

}
