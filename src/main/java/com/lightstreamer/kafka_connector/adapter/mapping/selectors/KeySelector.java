package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KeySelector<K> extends Selector {

    Value extract(String tag, ConsumerRecord<K, ?> record);

}
