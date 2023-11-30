package com.lightstreamer.kafka_connector.adapter.evaluator;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KeySelector<K> extends Selector {

    String expression();

    Value extract(ConsumerRecord<K, ?> record);

    default String expectedRoot() {
        return "KEY";
    }

}
