package com.lightstreamer.kafka_connector.adapter.evaluator;

public interface RecordEvaluator<T> {

    String extract(T t);

}
