package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.MetaSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Selector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.string.StringKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.string.StringValueSelectorSupplier;

public interface RecordInspector<K, V> {

    List<Value> inspect(ConsumerRecord<K, V> record);

    List<String> names();

    /**
     * Create a builder without {@link KeySelectorSupplier} either
     * {@link ValueSelectorSupplier}. Mainly used for test purposes.
     * 
     * @return a new {@code Builder}
     */
    static Builder<?, ?> noSelectorsBuilder() {
        return new Builder<>();
    }

    static <K, V> Builder<K, V> fakeSelectorBuidler() {
        return builder(new FakeKeySelectorSupplier<>(), new FakeValueSelectorSupplier<>());
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
                return metaSelectors.add(MetaSelector.of(name, expression));
            }
            return false;
        }

        public RecordInspector<K, V> build() {
            return new DefaultRecordInspector<>(
                    Collections.unmodifiableSet(metaSelectors),
                    Collections.unmodifiableSet(keySelectors),
                    Collections.unmodifiableSet(valueSelectors));
        }

    }
}

class DefaultRecordInspector<K, V> implements RecordInspector<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordInspector.class);

    private final Set<MetaSelector> metaSelectors;

    private final Set<KeySelector<K>> keySelectors;

    private final Set<ValueSelector<V>> valueSelectors;

    private final int valueSize;

    private final List<String> names;

    DefaultRecordInspector(
            Set<MetaSelector> ms,
            Set<KeySelector<K>> ks,
            Set<ValueSelector<V>> vs) {
        this.metaSelectors = ms;
        this.keySelectors = ks;
        this.valueSelectors = vs;
        this.valueSize = ms.size() + valueSelectors.size() + keySelectors.size();
        this.names = cacheNames();
    }

    @Override
    public List<String> names() {
        return names;
    }

    private List<String> cacheNames() {
        Stream<String> infoNames = metaSelectors.stream().map(Selector::name);
        Stream<String> keyNames = keySelectors.stream().map(Selector::name);
        Stream<String> valueNames = valueSelectors.stream().map(Selector::name);

        return Stream.of(infoNames, keyNames, valueNames).flatMap(i -> i).toList();
    }

    @Override
    public List<Value> inspect(ConsumerRecord<K, V> record) {
        Value[] values = new Value[valueSize];
        int c = 0;
        for (MetaSelector infoSelector : metaSelectors) {
            Value value = infoSelector.extract(record);
            values[c++] = value;
        }
        for (KeySelector<K> keySelector : keySelectors) {
            Value value = keySelector.extract(record);
            log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
            values[c++] = value;
        }
        for (ValueSelector<V> valueSelector : valueSelectors) {
            Value value = valueSelector.extract(record);
            log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
            values[c++] = value;
        }
        return Arrays.asList(values);
    }
}

class FakeKeySelectorSupplier<K> implements KeySelectorSupplier<K> {

    @Override
    public KeySelector<K> selector(String name, String expression) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'selector'");
    }
}

class FakeValueSelectorSupplier<V> implements ValueSelectorSupplier<V> {

    @Override
    public ValueSelector<V> selector(String name, String expression) {
        throw new UnsupportedOperationException("Unimplemented method 'selector'");
    }

    @Override
    public String deserializer(Properties pros) {
        throw new UnsupportedOperationException("Unimplemented method 'deserializer'");
    }

}

class FakeKeySelector<K> extends BaseSelector implements KeySelector<K> {

    protected FakeKeySelector(String name, String expression) {
        super(name, expression);
    }

    @Override
    public Value extract(ConsumerRecord<K, ?> record) {
        throw new UnsupportedOperationException("Unimplemented method 'extract'");
    }

}

class FakeValueSelector<V> extends BaseSelector implements ValueSelector<V> {

    protected FakeValueSelector(String name, String expression) {
        super(name, expression);
    }

    @Override
    public Value extract(ConsumerRecord<?, V> record) {
        throw new UnsupportedOperationException("Unimplemented method 'extract'");
    }

}
