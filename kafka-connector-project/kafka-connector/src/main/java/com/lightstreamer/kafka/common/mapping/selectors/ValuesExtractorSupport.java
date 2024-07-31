
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

class ValuesExtractorSupport {

    static class ValuesExtractorBuilder<K, V> implements ValuesExtractor.Builder<K, V> {

        private SelectorSuppliers<K, V> sSuppliers;

        private final Set<KeySelector<K>> keySelectors = new HashSet<>();
        private final Set<ValueSelector<V>> valueSelectors = new HashSet<>();
        private final Set<GeneralSelector> metaSelectors = new HashSet<>();

        private Map<String, ExtractionExpression> expressions = new HashMap<>();
        private String schemaName;

        ValuesExtractorBuilder(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
        }

        ValuesExtractorBuilder() {}

        @Override
        public ValuesExtractorBuilder<K, V> withSuppliers(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
            return this;
        }

        public ValuesExtractorBuilder<K, V> withExpressions(
                Map<String, ExtractionExpression> exprs) {
            expressions = Collections.unmodifiableMap(exprs);
            return this;
        }

        public ValuesExtractorBuilder<K, V> withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public ValuesExtractor<K, V> build() throws ExtractionException {
            SelectorsFiller<K, V> dispatcher = new SelectorsFiller<>(this);
            for (Map.Entry<String, ExtractionExpression> entry : expressions.entrySet()) {
                dispatcher.dispatch(entry);
            }
            return new ValuesExtractorImpl<>(this);
        }
    }

    private static class SelectorsFiller<K, V> {

        private static class Filler<T extends Selector> {

            private final Set<T> selectors;
            private final SelectorSupplier<T> selectorSupplier;

            Filler(Set<T> set, SelectorSupplier<T> selectorSupplier) {
                this.selectors = set;
                this.selectorSupplier = selectorSupplier;
            }

            void fill(String param, ExtractionExpression expression) throws ExtractionException {
                T newSelector = selectorSupplier.newSelector(param, expression);
                if (!selectors.add(newSelector)) {
                    throw ExtractionException.invalidExpression(param, expression.expression());
                }
            }
        }

        private Map<Constant, Filler<?>> fillers = new HashMap<>();

        SelectorsFiller(ValuesExtractorBuilder<K, V> builder) {
            KeySelectorSupplier<K> keySelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.keySelectorSupplier());
            ValueSelectorSupplier<V> valueSelectorSupplier =
                    Objects.requireNonNull(builder.sSuppliers.valueSelectorSupplier());
            GeneralSelectorSupplier metaSelectorSupplier = new GeneralSelectorSupplier();

            Filler<KeySelector<K>> kFiller =
                    new Filler<>(builder.keySelectors, keySelectorSupplier);
            Filler<ValueSelector<V>> vFiller =
                    new Filler<>(builder.valueSelectors, valueSelectorSupplier);
            Filler<GeneralSelector> mFiller =
                    new Filler<>(builder.metaSelectors, metaSelectorSupplier);

            fillers.put(Constant.KEY, kFiller);
            fillers.put(Constant.VALUE, vFiller);
            fillers.put(Constant.OFFSET, mFiller);
            fillers.put(Constant.TOPIC, mFiller);
            fillers.put(Constant.PARTITION, mFiller);
            fillers.put(Constant.TIMESTAMP, mFiller);
        }

        void dispatch(Map.Entry<String, ExtractionExpression> boundExpression)
                throws ExtractionException {
            String param = boundExpression.getKey();
            Filler<?> filler = fillers.get(boundExpression.getValue().root());
            filler.fill(param, boundExpression.getValue());
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
    }

    private ValuesExtractorSupport() {}
}
