package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ValueSelector<V> extends Selector {

    Value extract(ConsumerRecord<?, V> record);

}
