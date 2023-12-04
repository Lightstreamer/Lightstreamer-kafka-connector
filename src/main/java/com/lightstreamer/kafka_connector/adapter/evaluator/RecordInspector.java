package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.consumers.string.StringKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.consumers.string.StringValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.MetaSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelectorSupplier;

public interface RecordInspector<K, V> {

    List<Value> inspect(ConsumerRecord<K, V> record);

    List<String> names();

    Set<KeySelector<K>> keySelectors();

    Set<ValueSelector<V>> valueSelectors();

    Set<MetaSelector> metaSelectors();

    /**
     * Create a builder without {@link KeySelectorSupplier} either
     * {@link ValueSelectorSupplier}. Mainly used for test purposes.
     * 
     * @return a new {@code Builder}
     */
    static Builder<?, ?> noSelectorsBuilder() {
        return new Builder<>();
    }

    /**
     * Create a builder with the specified {@link KeySelectorSupplier} and
     * {@link ValueSelectorSupplier}.
     * 
     * @param keySupplier   the key supplier
     * @param valueSupplier the value supplier
     * @return a new {@code Builder}
     */
    static <K, V> Builder<K, V> builder(KeySelectorSupplier<K> keySupplier, ValueSelectorSupplier<V> valueSupplier) {
        return new Builder<>(keySupplier, valueSupplier);
    }

    static Builder<String, String> stringSelectorsBuilder() {
        return new Builder<>(new StringKeySelectorSupplier(), new StringValueSelectorSupplier());
    }

    public static class Builder<K, V> {

        private final KeySelectorSupplier<K> keySupplier;

        private final ValueSelectorSupplier<V> valueSupplier;

        private final Set<KeySelector<K>> keySelectors = new HashSet<>();

        private final Set<ValueSelector<V>> valueSelectors = new HashSet<>();

        private final Set<MetaSelector> metaSelectors = new HashSet<>();

        private Builder(KeySelectorSupplier<K> keySupplier, ValueSelectorSupplier<V> valueSupplier) {
            this.keySupplier = keySupplier;
            this.valueSupplier = valueSupplier;
        }

        private Builder() {
            this(null, null);
        }

        public Builder<K, V> instruct(String name, String expression) {
            boolean managed = false;
            managed |= addMetaSelector(name, expression);
            managed |= addKeySelector(name, expression);
            managed |= addValueSelector(name, expression);
            if (!managed) {
                throw new RuntimeException("Unknown expression <" + expression + ">");
            }

            return this;
        }

        protected boolean addKeySelector(String name, String expression) {
            if (expression.startsWith("KEY") && keySupplier != null) {
                KeySelector<K> keySelector = keySupplier.selector(name, expression);
                return keySelectors.add(keySelector);
            }
            return false;
        }

        protected boolean addValueSelector(String name, String expression) {
            if (expression.startsWith("VALUE") && valueSupplier != null) {
                ValueSelector<V> valueSelector = valueSupplier.selector(name, expression);
                return valueSelectors.add(valueSelector);
            }
            return false;
        }

        protected boolean addMetaSelector(String name, String expression) {
            if (List.of("TIMESTAMP", "TOPIC", "PARTITION").contains(expression)) {
                return metaSelectors.add(new MetaSelectorImpl(name, expression));
            }
            return false;
        }

        public RecordInspector<K, V> build() {
            return new BaseRecordInspector<>(
                    Collections.unmodifiableSet(metaSelectors),
                    Collections.unmodifiableSet(keySelectors),
                    Collections.unmodifiableSet(valueSelectors));
        }

    }
}
