package com.lightstreamer.kafka_connector.adapter.evaluator;

public interface ValueSelector<T> extends ValueSchema {

    String expression();

    Value extract(T t);

}
