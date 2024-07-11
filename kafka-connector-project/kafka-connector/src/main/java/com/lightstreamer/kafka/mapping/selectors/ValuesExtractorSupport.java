
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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

import com.lightstreamer.kafka.mapping.selectors.SelectorSupplier.Constant;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ValuesExtractorSupport {

    private ValuesExtractorSupport() {}

    private static class SelectorDispatcher<K, V> {

        private Map<Constant, Dispatcher<?>> dispatchers = new HashMap<>();

        private static class Dispatcher<T extends Selector> {

            private final Set<T> selectors;
            private final SelectorSupplier<T> selectorSupplier;

            Dispatcher(Set<T> set, SelectorSupplier<T> selectorSupplier) {
                this.selectors = set;
                this.selectorSupplier = selectorSupplier;
            }

            void dispatch(String param, String expression) {
                T newSelector = selectorSupplier.newSelector(param, expression);
                if (!selectors.add(newSelector)) {
                    throw ExpressionException.invalidExpression(param, expression);
                }
            }
        }

        SelectorDispatcher(ValuesExtractorBuilder<K, V> builder) {
            KeySelectorSupplier<K> keySelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.keySelectorSupplier());
            ValueSelectorSupplier<V> valueSelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.valueSelectorSupplier());
            GeneralSelectorSupplier metaSelectorSupplier = new GeneralSelectorSupplier();

            Dispatcher<KeySelector<K>> kDispatcher =
                    new Dispatcher<>(builder.keySelectors, keySelectorSupplier);
            Dispatcher<ValueSelector<V>> vDispatcher =
                    new Dispatcher<>(builder.valueSelectors, valueSelectorSupplier);
            Dispatcher<GeneralSelector> mDispatcher =
                    new Dispatcher<>(builder.metaSelectors, metaSelectorSupplier);

            dispatchers.put(Constant.KEY, kDispatcher);
            dispatchers.put(Constant.VALUE, vDispatcher);
            dispatchers.put(Constant.OFFSET, mDispatcher);
            dispatchers.put(Constant.TOPIC, mDispatcher);
            dispatchers.put(Constant.PARTITION, mDispatcher);
            dispatchers.put(Constant.TIMESTAMP, mDispatcher);
        }

        void dispatch(Map.Entry<String, String> boundExpression) throws ExpressionException {
            String param = boundExpression.getKey();
            String expression = boundExpression.getValue();
            String[] tokens = expression.split("\\.");
            Constant root = tokens.length > 0 ? Constant.from(tokens[0]) : null;
            if (root == null) {
                String t =
                        Arrays.stream(Constant.values())
                                .map(a -> a.toString())
                                .collect(Collectors.joining("|"));
                throw ExpressionException.expectedRootToken(param, t);
            }
            Dispatcher<?> dispatch = dispatchers.get(root);
            dispatch.dispatch(param, expression);
        }
    }

    static class ValuesExtractorBuilder<K, V> implements ValuesExtractor.Builder<K, V> {

        SelectorSuppliers<K, V> sSuppliers;

        final Set<KeySelector<K>> keySelectors = new HashSet<>();
        final Set<ValueSelector<V>> valueSelectors = new HashSet<>();
        final Set<GeneralSelector> metaSelectors = new HashSet<>();

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
            SelectorDispatcher<K, V> dispatcher = new SelectorDispatcher<>(this);
            expressions.entrySet().stream().forEach(dispatcher::dispatch);
            return new ValuesExtractorImpl<>(this);
        }
    }

    private static final class ValuesExtractorImpl<K, V> implements ValuesExtractor<K, V> {

        private final Set<KeySelector<K>> keySelectors;

        private final Set<ValueSelector<V>> valueSelectors;

        private final Set<GeneralSelector> metaSelectors;

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
                            .flatMap(identity())
                            .collect(toSet()));
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public ValuesContainer extractValues(KafkaRecord<K, V> record) throws ValueException {
            return new DefaultValuesContainer(
                    this,
                    Stream.of(
                                    keySelectors.stream().map(k -> k.extract(record)),
                                    valueSelectors.stream().map(v -> v.extract(record)),
                                    metaSelectors.stream().map(m -> m.extract(record)))
                            .flatMap(identity())
                            .collect(toSet()));
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
