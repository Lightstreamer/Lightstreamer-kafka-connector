package com.lightstreamer.kafka_connector.adapter.consumers.string;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

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
