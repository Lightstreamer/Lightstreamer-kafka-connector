
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

package com.lightstreamer.kafka.mapping.selectors;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ValuesExtractorSupport {

    private ValuesExtractorSupport() {}

    private interface SelectorProvider {

        boolean add(String param, String expression);
    }

    private static class SelectorProviders<K, V> {

        private SelectorSupplier<MetaSelector> metaSelectorSupplier = new MetaSelectorSupplier();
        private ValuesExtractorBuilder<K, V> builder;
        private KeySelectorSupplier<K> keySelectorSupplier;
        private ValueSelectorSupplier<V> valueSelectorSupplier;
        private final SelectorProvider rootProvider;

        private abstract sealed class SelectorProviderChain implements SelectorProvider
                permits KeySelectorProvider, ValueSelectorProvider, MetaSelectorProvider {

            private final SelectorProvider ref;

            SelectorProviderChain(SelectorProvider ref) {
                this.ref = ref;
            }

            SelectorProviderChain() {
                this(null);
            }

            @Override
            public boolean add(String param, String expression) {
                boolean added = false;
                if (ref != null) {
                    added = ref.add(param, expression);
                }
                if (!added) {
                    return doAdd(param, expression);
                }
                return added;
            }

            abstract boolean doAdd(String param, String expression);
        }

        private final class KeySelectorProvider extends SelectorProviderChain {

            KeySelectorProvider(SelectorProvider ref) {
                super(ref);
            }

            @Override
            boolean doAdd(String param, String expression) {
                if (keySelectorSupplier.maySupply(expression)) {
                    return builder.keySelectors.add(
                            keySelectorSupplier.newSelector(param, expression));
                }
                return false;
            }
        }

        private final class ValueSelectorProvider extends SelectorProviderChain {

            @Override
            boolean doAdd(String param, String expression) {
                if (valueSelectorSupplier.maySupply(expression)) {
                    return builder.valueSelectors.add(
                            valueSelectorSupplier.newSelector(param, expression));
                }
                return false;
            }
        }

        private final class MetaSelectorProvider extends SelectorProviderChain {

            MetaSelectorProvider(SelectorProvider ref) {
                super(ref);
            }

            @Override
            boolean doAdd(String param, String expression) {
                if (metaSelectorSupplier.maySupply(expression)) {
                    return builder.metaSelectors.add(
                            metaSelectorSupplier.newSelector(param, expression));
                }
                return false;
            }
        }

        SelectorProviders(ValuesExtractorBuilder<K, V> builder) {
            this.builder = builder;
            this.keySelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.keySelectorSupplier());
            this.valueSelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.valueSelectorSupplier());
            this.rootProvider =
                    new MetaSelectorProvider(new KeySelectorProvider(new ValueSelectorProvider()));
        }

        public boolean add(String param, String expression) {
            return rootProvider.add(param, expression);
        }
    }

    static class ValuesExtractorBuilder<K, V> implements ValuesExtractor.Builder<K, V> {

        SelectorSuppliers<K, V> sSuppliers;

        final Set<KeySelector<K>> keySelectors = new HashSet<>();
        final Set<ValueSelector<V>> valueSelectors = new HashSet<>();
        final Set<MetaSelector> metaSelectors = new HashSet<>();

        private Map<String, String> expressions = new HashMap<>();

        String schemaName;

        ValuesExtractorBuilder(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
        }

        ValuesExtractorBuilder() {}

        @Override
        public ValuesExtractorBuilder<K, V> withSuppliers(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
            return this;
        }

        public ValuesExtractorBuilder<K, V> withExpressions(Map<String, String> exprs) {
            expressions = Collections.unmodifiableMap(exprs);
            return this;
        }

        public ValuesExtractorBuilder<K, V> withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public ValuesExtractor<K, V> build() throws ExpressionException {
            SelectorProviders<K, V> selectorProviders = new SelectorProviders<>(this);
            expressions.entrySet().stream()
                    .forEach(
                            e -> {
                                String param = e.getKey();
                                String value = e.getValue();
                                if (!selectorProviders.add(param, value)) {
                                    ExpressionException.throwInvalidExpression(param, value);
                                }
                            });
            return new ValuesExtractorImpl<>(this);
        }
    }

    private static final class ValuesExtractorImpl<K, V> implements ValuesExtractor<K, V> {

        private final Set<KeySelector<K>> keySelectors;

        private final Set<ValueSelector<V>> valueSelectors;

        private final Set<MetaSelector> metaSelectors;

        private final Schema schema;

        ValuesExtractorImpl(ValuesExtractorBuilder<K, V> builder) {
            this.keySelectors = Collections.unmodifiableSet(builder.keySelectors);
            this.valueSelectors = Collections.unmodifiableSet(builder.valueSelectors);
            this.metaSelectors = Collections.unmodifiableSet(builder.metaSelectors);
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

            return obj instanceof ValuesExtractorImpl<?, ?> other
                    && Objects.equals(keySelectors, other.keySelectors)
                    && Objects.equals(valueSelectors, other.valueSelectors)
                    && Objects.equals(metaSelectors, other.metaSelectors)
                    && Objects.equals(schema, other.schema);
        }
    }
}
