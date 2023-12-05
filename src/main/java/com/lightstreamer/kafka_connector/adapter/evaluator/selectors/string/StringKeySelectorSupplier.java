package com.lightstreamer.kafka_connector.adapter.evaluator.selectors.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

public final class StringKeySelectorSupplier implements KeySelectorSupplier<String> {

    static final class StringKeySelector extends StringBaseSelector implements KeySelector<String> {

        private StringKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<String, ?> record) {
            return Value.of(name(), record.key());
        }

    }

    public StringKeySelectorSupplier() {
    }

    @Override
    public KeySelector<String> selector(String name, String expression) {
        return new StringKeySelector(name, expression);
    }

}
