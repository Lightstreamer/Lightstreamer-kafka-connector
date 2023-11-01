package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.consumers.raw.RawValueSelector2;

public interface RecordInspector<K, V> {

    List<Value> inspect(ConsumerRecord<K, V> record);

    List<String> names();

    List<Selector<V>> valueSelectors();

    List<Selector<K>> keySelectors();

    List<Selector<ConsumerRecord<?,?>>> infoSelectors();

    public static class Builder<K, V> {

        private final SelectorSupplier<K> keySupplier;

        private final SelectorSupplier<V> valueSupplier;

        private final List<Selector<K>> keySelectors = new ArrayList<>();

        private final List<Selector<V>> valueSelectors = new ArrayList<>();

        private final List<Selector<ConsumerRecord<?, ?>>> infoSelectors = new ArrayList<>();

        public Builder(SelectorSupplier<K> keySupplier, SelectorSupplier<V> valueSupplier) {
            this.keySupplier = keySupplier;
            this.valueSupplier = valueSupplier;
        }

        public Builder<K, V> instruct(String name, String expression) {
            if (List.of("TIMESTAMP", "TOPIC", "PARTITION").contains(expression)) {
                // infoSelectors.add(new InfoSelector(name, expression));
                infoSelectors.add(new RawValueSelector2(name, expression));
            }

            if (expression.startsWith("KEY.") || expression.equals("KEY")) {
                Selector<K> keySelector = keySupplier.get(name, expression);
                keySelectors.add(keySelector);
            }

            if (expression.startsWith("VALUE.") || expression.equals("VALUE")) {
                Selector<V> valueSelector = valueSupplier.get(name, expression);
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
