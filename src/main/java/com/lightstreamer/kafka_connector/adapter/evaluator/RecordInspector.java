package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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

    Map<String, String> inspect(ConsumerRecord<K, V> record);

    Set<String> names();

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

        private final Map<String, Selector> selectors = new HashMap<>();

        private Builder<K, V>.MetaSelectorManager metaSelectorExprMgr;

        private Builder<K, V>.KeySelectorManager keySelectorExprMgr;

        private Builder<K, V>.ValueSelectorManager valueSelectorExprMgrt;

        private interface SelectorManager {

            boolean manage(String name, String expression);

        }

        private abstract class SelectorManagerChain implements SelectorManager {

            private SelectorManager ref;

            void setDelegate(SelectorManager ref) {
                this.ref = ref;
            }

            @Override
            public boolean manage(String name, String expression) {
                boolean ret = false;
                if (ref != null) {
                    ret = ref.manage(name, expression);
                }
                if (!ret) {
                    return doManage(name, expression);
                }
                return ret;
            }

            abstract boolean doManage(String name, String expression);

        }

        private class KeySelectorManager extends SelectorManagerChain {

            @Override
            public boolean doManage(String name, String expression) {
                return addKeySelector(name, expression);
            }
        }

        private class ValueSelectorManager extends SelectorManagerChain {

            @Override
            public boolean doManage(String name, String expression) {
                return addValueSelector(name, expression);
            }
        }

        private class MetaSelectorManager extends SelectorManagerChain {

            @Override
            public boolean doManage(String name, String expression) {
                return addMetaSelector(name, expression);
            }
        }

        private Builder(KeySelectorSupplier<K> keySupplier, ValueSelectorSupplier<V> valueSupplier) {
            this.keySupplier = keySupplier;
            this.valueSupplier = valueSupplier;
            this.metaSelectorExprMgr = new MetaSelectorManager();
            this.keySelectorExprMgr = new KeySelectorManager();
            this.valueSelectorExprMgrt = new ValueSelectorManager();
            metaSelectorExprMgr.setDelegate(keySelectorExprMgr);
            keySelectorExprMgr.setDelegate(valueSelectorExprMgrt);
        }

        private Builder() {
            this(null, null);
        }

        public Builder<K, V> instruct(String name, String expression) {
            boolean managed = metaSelectorExprMgr.manage(name, expression);
            if (!managed) {
                throw new RuntimeException("Unknown expression <" + expression + ">");
            }

            return this;
        }

        protected boolean addKeySelector(String name, String expression) {
            if (expression.startsWith("KEY") && keySupplier != null) {
                return checkAndAdd(keySelectors, keySupplier.selector(name, expression));
            }
            return false;
        }

        protected boolean addValueSelector(String name, String expression) {
            if (expression.startsWith("VALUE") && valueSupplier != null) {
                return checkAndAdd(valueSelectors, valueSupplier.selector(name, expression));
            }
            return false;
        }

        protected boolean addMetaSelector(String name, String expression) {
            if (List.of("TIMESTAMP", "TOPIC", "PARTITION").contains(expression)) {
                return checkAndAdd(metaSelectors, MetaSelector.of(name, expression));
            }
            return false;
        }

        private <T> boolean checkAndAdd(Set<T> set, T value) {
            if (!metaSelectors.contains(value) &&
                    !keySelectors.contains(value) &&
                    !valueSelectors.contains(value)) {
                return set.add(value);
            }
            throw new RuntimeException("Duplicated entry");
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

    private final Set<String> names;

    private Map<String, Selector> s = new HashMap<>();

    DefaultRecordInspector(Set<MetaSelector> ms, Set<KeySelector<K>> ks, Set<ValueSelector<V>> vs) {
        this.metaSelectors = ms;
        this.keySelectors = ks;
        this.valueSelectors = vs;
        this.names = cacheNames();

        metaSelectors.stream().forEach(m -> s.put(m.name(), m));
        valueSelectors.stream().forEach(m -> s.put(m.name(), m));
        keySelectors.stream().forEach(m -> s.put(m.name(), m));
    }

    @Override
    public Set<String> names() {
        return names;
    }

    private Set<String> cacheNames() {
        Stream<String> infoNames = metaSelectors.stream().map(Selector::name);
        Stream<String> keyNames = keySelectors.stream().map(Selector::name);
        Stream<String> valueNames = valueSelectors.stream().map(Selector::name);

        return Stream.of(infoNames, keyNames, valueNames)
                .flatMap(Function.identity())
                .collect(Collectors.toSet());
    }

    @Override
    public Map<String, String> inspect(ConsumerRecord<K, V> record) {
        Collection<Selector> values2 = s.values();

        Map<String, String> values = new HashMap<>();
        for (Selector v : values2) {
            Value value = switch (v) {
                case KeySelector ks -> ks.extract(record);

                case ValueSelector vs -> vs.extract(record);

                case MetaSelector ms -> ms.extract(record);

                default -> {
                    throw new RuntimeException("Error");
                }
            };
            values.put(value.name(), value.text());
        }
        return values;

        // for (MetaSelector infoSelector : metaSelectors) {
        // Value value = infoSelector.extract(record);
        // values[c++] = value;
        // }
        // for (KeySelector<K> keySelector : keySelectors) {
        // Value value = keySelector.extract(record);
        // log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
        // values[c++] = value;
        // }
        // for (ValueSelector<V> valueSelector : valueSelectors) {
        // Value value = valueSelector.extract(record);
        // log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
        // values[c++] = value;
        // }
        // return Arrays.asList(values);
    }

}

class FakeKeySelectorSupplier<K> implements KeySelectorSupplier<K> {

    @Override
    public KeySelector<K> selector(String name, String expression) {
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
