package com.lightstreamer.kafka_connector.adapter.evaluator;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ValueSelector<K, V> extends ValueSchema {

    String expression();

    Value extract(ConsumerRecord<K, V> record);

}
