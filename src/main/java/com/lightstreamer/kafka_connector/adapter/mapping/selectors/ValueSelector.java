package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.SchemaName;

public interface ValueSelector<V> extends Selector {

    Value extract(SchemaName schemaName, ConsumerRecord<?, V> record);

}
