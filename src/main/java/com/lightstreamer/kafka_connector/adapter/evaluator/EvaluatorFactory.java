package com.lightstreamer.kafka_connector.adapter.evaluator;

@FunctionalInterface
public interface EvaluatorFactory<T> {

    RecordEvaluator<T> get(String stmt);

}
