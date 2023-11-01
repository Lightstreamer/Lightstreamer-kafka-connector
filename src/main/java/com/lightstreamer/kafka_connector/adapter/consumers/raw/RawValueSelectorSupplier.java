package com.lightstreamer.kafka_connector.adapter.consumers.raw;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.Selector;

public final class RawValueSelectorSupplier extends AbstractSelectorSupplier<ConsumerRecord<?,?>> {

    public RawValueSelectorSupplier(Map<String, String> conf, boolean isKey) {
        if (isKey) {
            configKeyDeserializer(conf);
        } else {
            configValueDeserializer(conf);
        }
    }

    @Override
    public Selector<ConsumerRecord<?,?>> get(String name, String expression) {
        return new RawValueSelector2(name, expression);
    }
}
