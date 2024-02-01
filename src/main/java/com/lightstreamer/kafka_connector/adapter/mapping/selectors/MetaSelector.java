/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MetaSelector extends Selector {

  Value extract(ConsumerRecord<?, ?> record) throws ValueException;
}
