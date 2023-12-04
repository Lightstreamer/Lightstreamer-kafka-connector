package com.lightstreamer.kafka_connector.adapter.evaluator.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MetaSelector extends Selector {

    Value extract(ConsumerRecord<?, ?> record);

}
