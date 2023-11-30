package com.lightstreamer.kafka_connector.adapter.evaluator;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ValueSelector<V> extends Selector {

    Value extract(ConsumerRecord<?, V> record);

    default String expectedRoot() {
        return "VALUE";
    }

}
