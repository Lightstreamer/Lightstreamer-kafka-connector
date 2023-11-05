package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.consumers.raw.RawValueSelector2;

public interface RecordInspector<K, V> {

    List<Value> inspect(ConsumerRecord<K, V> record);

    List<String> names();

    List<KeySelector<K>> keySelectors();

    List<ValueSelector<V>> valueSelectors();

    List<Selector<ConsumerRecord<?,?>>> infoSelectors();

    public static class Builder<K, V> {

        private final KeySelectorSupplier<K> keySupplier;

        private final ValueSelectorSupplier<V> valueSupplier;

        private final List<KeySelector<K>> keySelectors = new ArrayList<>();

        private final List<ValueSelector<V>> valueSelectors = new ArrayList<>();

        private final List<Selector<ConsumerRecord<?, ?>>> infoSelectors = new ArrayList<>();

        public Builder(KeySelectorSupplier<K> keySupplier, ValueSelectorSupplier<V> valueSupplier) {
            this.keySupplier = keySupplier;
            this.valueSupplier = valueSupplier;
        }

        public Builder<K, V> instruct(String name, String expression) {
            if (List.of("TIMESTAMP", "TOPIC", "PARTITION").contains(expression)) {
                // infoSelectors.add(new InfoSelector(name, expression));
                infoSelectors.add(new RawValueSelector2(name, expression));
            }

            if (expression.startsWith("KEY.") || expression.equals("KEY")) {
                KeySelector<K> keySelector = keySupplier.selector(name, expression);
                keySelectors.add(keySelector);
            }

            if (expression.startsWith("VALUE.") || expression.equals("VALUE")) {
                ValueSelector<V> valueSelector = valueSupplier.selector(name, expression);
                valueSelectors.add(valueSelector);
            }

            return this;
        }

        public RecordInspector<K, V> build() {
            return new BaseRecordInspector<>(
                    Collections.unmodifiableList(infoSelectors),
                    Collections.unmodifiableList(keySelectors),
                    Collections.unmodifiableList(valueSelectors));
        }

    }

}
