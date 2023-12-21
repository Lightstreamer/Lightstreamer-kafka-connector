package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.MetaSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.MetaSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GeneircRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;

public interface Selectors<K, V> {

    interface SelectorsSupplier<K, V> {

        KeySelectorSupplier<K> keySelectorSupplier();

        ValueSelectorSupplier<V> valueSelectorSupplier();

        static <K, V> SelectorsSupplier<K, V> wrap(KeySelectorSupplier<K> k, ValueSelectorSupplier<V> v) {
            return new DefautlSelectorSupplier<>(k, v);
        }

        static SelectorsSupplier<String, String> string() {
            return wrap(StringSelectorSuppliers.keySelectorSupplier(), StringSelectorSuppliers.valueSelectorSupplier());
        }

        static SelectorsSupplier<GenericRecord, GenericRecord> genericRecord() {
            return wrap(GeneircRecordSelectorsSuppliers.keySelectorSupplier(),
                    GeneircRecordSelectorsSuppliers.valueSelectorSupplier());
        }

        static SelectorsSupplier<JsonNode, JsonNode> jsonNode() {
            return wrap(JsonNodeSelectorsSuppliers.keySelectorSupplier(),
                    JsonNodeSelectorsSuppliers.valueSelectorSupplier());
        }

    }

    Set<Value> extractValues(ConsumerRecord<K, V> record);

    Schema schema();

    static <K, V> Selectors<K, V> from(SelectorsSupplier<K, V> selectorsSupplier, Map<String, String> entries) {
        return builder(selectorsSupplier).withMap(entries).build();
    }

    private static <K, V> Builder<K, V> builder(SelectorsSupplier<K, V> selectorsSupplier) {
        return new Builder<>(selectorsSupplier.keySelectorSupplier(), selectorsSupplier.valueSelectorSupplier());
    }

    static class Builder<K, V> {

        private final ValueSelectorSupplier<V> valueSupplier;

        private final KeySelectorSupplier<K> keySupplier;

        private final Set<KeySelector<K>> keySelectors = new HashSet<>();

        private final Set<ValueSelector<V>> valueSelectors = new HashSet<>();

        private final Set<MetaSelector> metaSelectors = new HashSet<>();

        private final KeySelectorManager keySelectorExprMgr;

        private final ValueSelectorManager valueSelectorExprMgrt;

        private final MetaSelectorManager metaSelectorExprMgr;

        private final MetaSelectorSupplier metaSelectorSupplier;

        private final Map<String, String> entries = new HashMap<>();

        private Builder(KeySelectorSupplier<K> ks, ValueSelectorSupplier<V> vs) {
            this.keySupplier = Objects.requireNonNull(ks);
            this.valueSupplier = Objects.requireNonNull(vs);
            this.metaSelectorSupplier = new MetaSelectorSupplier();
            this.metaSelectorExprMgr = new MetaSelectorManager();
            this.keySelectorExprMgr = new KeySelectorManager();
            this.valueSelectorExprMgrt = new ValueSelectorManager();

            metaSelectorExprMgr.setDelegate(keySelectorExprMgr);
            keySelectorExprMgr.setDelegate(valueSelectorExprMgrt);
        }

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
                if (keySupplier.maySupply(expression)) {
                    return keySelectors.add(keySupplier.newSelector(name, expression));
                }
                return false;
            }
        }

        private class ValueSelectorManager extends SelectorManagerChain {

            @Override
            public boolean doManage(String name, String expression) {
                if (valueSupplier.maySupply(expression)) {
                    return valueSelectors.add(valueSupplier.newSelector(name, expression));
                }
                return false;
            }
        }

        private class MetaSelectorManager extends SelectorManagerChain {

            @Override
            public boolean doManage(String name, String expression) {
                if (metaSelectorSupplier.maySupply(expression)) {
                    return metaSelectors.add(metaSelectorSupplier.newSelector(name, expression));
                }
                return false;
            }
        }

        public Builder<K, V> withMap(Map<String, String> map) {
            map.entrySet().stream().forEach(this::withEntry);
            return this;
        }

        Builder<K, V> withEntry(Map.Entry<String, String> entry) {
            withEntry(entry.getKey(), entry.getValue());
            return this;
        }

        public Builder<K, V> withEntry(String name, String expression) {
            if (entries.put(name, expression) != null) {
                throw new ExpressionException("Key \"" + name + "\" already present");
            }
            return this;
        }

        public Selectors<K, V> build() {
            entries.entrySet().stream().forEach(e -> {
                if (!metaSelectorExprMgr.manage(e.getKey(), e.getValue())) {
                    ExpressionException.throwInvalidExpression();
                }
            });

            return new DefaultSelectors<>(keySelectors, valueSelectors, metaSelectors);
        }
    }
}

record DefaultSelectors<K, V>(Set<KeySelector<K>> keySelectors, Set<ValueSelector<V>> valueSelectors,
        Set<MetaSelector> metaSelectors) implements Selectors<K, V> {

    public Schema schema() {
        Stream<String> infoNames = metaSelectors().stream().map(Selector::name);
        Stream<String> keyNames = keySelectors().stream().map(Selector::name);
        Stream<String> valueNames = valueSelectors().stream().map(Selector::name);

        return Schema.of(Stream.of(infoNames, keyNames, valueNames)
                .flatMap(Function.identity())
                .collect(Collectors.toSet()));
    }

    public Set<Value> extractValues(ConsumerRecord<K, V> record) {
        return Stream.of(
                keySelectors.stream().map(k -> k.extract(record)),
                valueSelectors.stream().map(v -> v.extract(record)),
                metaSelectors.stream().map(m -> m.extract(record)))
                .flatMap(Function.identity())
                .collect(Collectors.toSet());
    }

}

record DefautlSelectorSupplier<K, V>(KeySelectorSupplier<K> keySelectorSupplier,
        ValueSelectorSupplier<V> valueSelectorSupplier) implements Selectors.SelectorsSupplier<K, V> {

}