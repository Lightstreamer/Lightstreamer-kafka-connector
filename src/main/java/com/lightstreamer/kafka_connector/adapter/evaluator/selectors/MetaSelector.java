package com.lightstreamer.kafka_connector.adapter.evaluator.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MetaSelector extends Selector {

    Value extract(ConsumerRecord<?, ?> record);

    static MetaSelector of(String name, String expression) {
        return new MetaSelectorImpl(name, expression);
    }

}
