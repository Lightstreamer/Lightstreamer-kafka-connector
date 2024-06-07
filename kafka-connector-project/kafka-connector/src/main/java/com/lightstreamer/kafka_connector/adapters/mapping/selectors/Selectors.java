
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka_connector.adapters.mapping.selectors;

import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Selectors<K, V> {

    interface Selected<K, V> {

        KeySelectorSupplier<K> keySelectorSupplier();

        ValueSelectorSupplier<V> valueSelectorSupplier();

        static <K, V> Selected<K, V> with(KeySelectorSupplier<K> k, ValueSelectorSupplier<V> v) {
            return new DefautlSelectorSupplier<>(k, v);
        }
    }

    ValuesContainer extractValues(KafkaRecord<K, V> record);

    Schema schema();

    static <K, V> Selectors<K, V> from(
            Selected<K, V> selected, String schemaName, Map<String, String> entries)
            throws ExpressionException {
        return builder(selected).withMap(entries).withSchemaName(schemaName).build();
    }

    public static <K, V> Builder<K, V> builder(Selected<K, V> selected) {
        return new Builder<>(selected.keySelectorSupplier(), selected.valueSelectorSupplier());
    }

    public static class Builder<K, V> {

        private final ValueSelectorSupplier<V> valueSupplier;

        private final KeySelectorSupplier<K> keySupplier;

        private final KeySelectorManager keySelectorExprMgr;

        private final ValueSelectorManager valueSelectorExprMgrt;

        private final MetaSelectorManager metaSelectorExprMgr;

        private final MetaSelectorSupplier metaSelectorSupplier;

        private final Map<EntryName, EntryValue> entries = new HashMap<>();

        String schemaName;

        final Set<KeySelector<K>> keySelectors = new HashSet<>();

        final Set<ValueSelector<V>> valueSelectors = new HashSet<>();

        final Set<MetaSelector> metaSelectors = new HashSet<>();

        private static record EntryName(String originalName, String name) {}

        private static record EntryValue(String originalValue, String value) {}

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

        public Builder<K, V> withEntry(Map.Entry<String, String> entry) {
            withEntry(entry.getKey(), entry.getValue());
            return this;
        }

        public Builder<K, V> withEntry(String name, String expression) {
            return withEntry(name, name, expression, expression);
        }

        public Builder<K, V> withEntry(
                String originalName, String name, String originalExpression, String expression) {
            if (entries.put(
                            new EntryName(originalName, name),
                            new EntryValue(originalExpression, expression))
                    != null) {
                throw new ExpressionException("Key \"" + originalName + "\" already present");
            }
            return this;
        }

        public Builder<K, V> withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Selectors<K, V> build() throws ExpressionException {
            entries.entrySet().stream()
                    .forEach(
                            e -> {
                                EntryName key = e.getKey();
                                EntryValue value = e.getValue();
                                if (!metaSelectorExprMgr.manage(key.name(), value.value())) {
                                    ExpressionException.throwInvalidExpression(
                                            key.originalName(), value.originalValue());
                                }
                            });

            return new DefaultSelectors<>(this);
        }
    }
}

class DefaultSelectors<K, V> implements Selectors<K, V> {

    private final Set<KeySelector<K>> keySelectors;

    private final Set<ValueSelector<V>> valueSelectors;

    private final Set<MetaSelector> metaSelectors;

    private final Schema schema;

    DefaultSelectors(Selectors.Builder<K, V> builder) {
        this.keySelectors = builder.keySelectors;
        this.valueSelectors = builder.valueSelectors;
        this.metaSelectors = builder.metaSelectors;
        this.schema = mkSchema(builder.schemaName);
    }

    private Schema mkSchema(String schemaName) {
        Stream<String> keyNames = keySelectors.stream().map(Selector::name);
        Stream<String> valueNames = valueSelectors.stream().map(Selector::name);
        Stream<String> metaNames = metaSelectors.stream().map(Selector::name);

        return Schema.from(
                schemaName,
                Stream.of(metaNames, keyNames, valueNames)
                        .flatMap(Function.identity())
                        .collect(Collectors.toSet()));
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public ValuesContainer extractValues(KafkaRecord<K, V> record) {
        return new DefaultValuesContainer(
                this,
                Stream.of(
                                keySelectors.stream().map(k -> k.extract(record)),
                                valueSelectors.stream().map(v -> v.extract(record)),
                                metaSelectors.stream().map(m -> m.extract(record)))
                        .flatMap(Function.identity())
                        .collect(Collectors.toSet()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySelectors, valueSelectors, metaSelectors, schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof DefaultSelectors<?, ?> other
                && Objects.equals(keySelectors, other.keySelectors)
                && Objects.equals(valueSelectors, other.valueSelectors)
                && Objects.equals(metaSelectors, other.metaSelectors)
                && Objects.equals(schema, other.schema);
    }
}

record DefautlSelectorSupplier<K, V>(
        KeySelectorSupplier<K> keySelectorSupplier, ValueSelectorSupplier<V> valueSelectorSupplier)
        implements Selectors.Selected<K, V> {}
