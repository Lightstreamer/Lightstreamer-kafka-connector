package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

public final class StringValueSelectorSupplier implements ValueSelectorSupplier<String> {

    static class StringValueSelector extends BaseSelector implements ValueSelector<String> {

        private StringValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, String> record) {
            return Value.of(name(), record.value());
        }
    }

    @Override
    public ValueSelector<String> selector(String name, String expression) {
        if (!maySupply(expression)) {
            ExpressionException.throwExpectedToken(expectedRoot());
        }
        return new StringValueSelector(name, expression);
    }

    @Override
    public boolean maySupply(String expression) {
        return expression.equals(expectedRoot());
    }
}
