package com.lightstreamer.kafka_connector.adapter.consumers.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelectorSupplier;

public final class StringValueSelectorSupplier implements ValueSelectorSupplier<String> {

    static class StringValueSelector extends StringBaseSelector implements ValueSelector<String> {

        private StringValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        protected String checkExpression(String expression) {
            if (expectedRoot().equals(expression)) {
                return expression;
            }

            throw new RuntimeException("Expected " + expectedRoot());
        }

        @Override
        public Value extract(ConsumerRecord<?, String> record) {
            return Value.of(name(), record.value());

        }
    }

    public StringValueSelectorSupplier() {
    }

    @Override
    public ValueSelector<String> selector(String name, String expression) {
        return new StringValueSelector(name, expression);
    }
}
