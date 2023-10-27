package com.lightstreamer.kafka_connector.adapter.consumers.raw;

import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.Selector;

public class RawValueSelectorSupplier extends AbstractSelectorSupplier<String> {

    public RawValueSelectorSupplier(Map<String, String> conf, boolean isKey) {
        if (isKey) {
            configKeyDeserializer(conf);
        } else {
            configValueDeserializer(conf);
        }
    }

    @Override
    public Selector<String> get(String name, String expression) {
        return new RawValueSelector(name);
    }
}
