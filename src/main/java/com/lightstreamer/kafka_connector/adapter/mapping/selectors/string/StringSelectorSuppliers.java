package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

public class StringSelectorSuppliers {

    private static final StringKeySelectorSupplier KEY_SELCTOR_SUPPLIER = new StringKeySelectorSupplier();

    private static final StringValueSelectorSupplier VALUE_SELECTOR_SUPPLIER = new StringValueSelectorSupplier();

    public static ValueSelectorSupplier<String> valueSelectorSupplier() {
        return VALUE_SELECTOR_SUPPLIER;
    }

    public static KeySelectorSupplier<String> keySelectorSupplier() {
        return KEY_SELCTOR_SUPPLIER;
    }

    private static class StringKeySelectorSupplier implements KeySelectorSupplier<String> {

        @Override
        public boolean maySupply(String expression) {
            return expectedRoot().equals(expression);
        }

        @Override
        public KeySelector<String> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                ExpressionException.throwExpectedToken(expectedRoot());
            }
            return new StringKeySelector(name, expression);
        }
    }

    private static final class StringKeySelector extends BaseSelector implements KeySelector<String> {

        private StringKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(String tag, ConsumerRecord<String, ?> record) {
            return Value.of(tag, name(), record.key());
        }
    }

    private static class StringValueSelectorSupplier implements ValueSelectorSupplier<String> {

        @Override
        public boolean maySupply(String expression) {
            return expression.equals(expectedRoot());
        }

        @Override
        public ValueSelector<String> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                ExpressionException.throwExpectedToken(expectedRoot());
            }
            return new StringValueSelector(name, expression);
        }
    }

    private static class StringValueSelector extends BaseSelector implements ValueSelector<String> {

        private StringValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(String tag, ConsumerRecord<?, String> record) {
            return Value.of(tag, name(), record.value());
        }
    }

}
