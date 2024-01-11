package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.SchemaName;

public interface KeySelector<K> extends Selector {

    Value extract(SchemaName schemaName, ConsumerRecord<K, ?> record);

}
