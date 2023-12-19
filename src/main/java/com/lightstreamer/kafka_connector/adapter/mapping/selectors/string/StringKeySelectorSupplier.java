package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

public final class StringKeySelectorSupplier implements KeySelectorSupplier<String> {

    static final class StringKeySelector extends BaseSelector implements KeySelector<String> {

        private StringKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<String, ?> record) {
            return Value.of(name(), record.key());
        }
    }

    @Override
    public KeySelector<String> selector(String name, String expression) {
        if (!maySupply(expression)) {
            ExpressionException.throwExpectedToken(expectedRoot());
        }
        return new StringKeySelector(name, expression);
    }

    @Override
    public boolean maySupply(String expression) {
        return expectedRoot().equals(expression);
    }
}
