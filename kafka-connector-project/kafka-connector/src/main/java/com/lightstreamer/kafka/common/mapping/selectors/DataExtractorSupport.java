
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

class DataExtractorSupport {

    static class DataExtractorBuilder<K, V> implements DataExtractor.Builder<K, V> {

        private SelectorSuppliers<K, V> sSuppliers;

        private final Set<KeySelector<K>> keySelectors = new HashSet<>();
        private final Set<ValueSelector<V>> valueSelectors = new HashSet<>();
        private final Set<ConstantSelector> metaSelectors = new HashSet<>();

        private Map<String, ExtractionExpression> expressions = new HashMap<>();
        private String schemaName;

        DataExtractorBuilder(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
        }

        DataExtractorBuilder() {}

        @Override
        public DataExtractorBuilder<K, V> withSuppliers(SelectorSuppliers<K, V> sSuppliers) {
            this.sSuppliers = sSuppliers;
            return this;
        }

        public DataExtractorBuilder<K, V> withExpressions(Map<String, ExtractionExpression> exprs) {
            expressions = Collections.unmodifiableMap(exprs);
            return this;
        }

        public DataExtractorBuilder<K, V> withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public DataExtractor<K, V> build() throws ExtractionException {
            SelectorsAppender<K, V> dispatcher = new SelectorsAppender<>(this);
            for (Map.Entry<String, ExtractionExpression> entry : expressions.entrySet()) {
                dispatcher.dispatch(entry);
            }
            return new DataExtractorImpl<>(this);
        }
    }

    private static class SelectorsAppender<K, V> {

        private static class Appender<T extends Selector> {

            private final Set<T> selectors;
            private final SelectorSupplier<T> selectorSupplier;

            Appender(Set<T> set, SelectorSupplier<T> selectorSupplier) {
                this.selectors = set;
                this.selectorSupplier = selectorSupplier;
            }

            void append(String param, ExtractionExpression expression) throws ExtractionException {
                T newSelector = selectorSupplier.newSelector(param, expression);
                if (!selectors.add(newSelector)) {
                    throw ExtractionException.invalidExpression(param, expression.expression());
                }
            }
        }

        private Appender<KeySelector<K>> kFiller;
        private Appender<ValueSelector<V>> vFiller;
        private Appender<ConstantSelector> mFiller;

        SelectorsAppender(DataExtractorBuilder<K, V> builder) {
            kFiller =
                    new Appender<>(
                            builder.keySelectors,
                            Objects.requireNonNull(builder.sSuppliers.keySelectorSupplier()));
            vFiller =
                    new Appender<>(
                            builder.valueSelectors,
                            Objects.requireNonNull(builder.sSuppliers.valueSelectorSupplier()));
            mFiller =
                    new Appender<>(
                            builder.metaSelectors,
                            new ConstantSelectorSupplier(
                                    Constant.OFFSET,
                                    Constant.PARTITION,
                                    Constant.TIMESTAMP,
                                    Constant.TOPIC));
        }

        void dispatch(Map.Entry<String, ExtractionExpression> boundExpression)
                throws ExtractionException {
            Constant root = boundExpression.getValue().constant();
            Appender<?> filler =
                    switch (root) {
                        case KEY -> kFiller;
                        case VALUE -> vFiller;
                        default -> mFiller;
                    };
            filler.append(boundExpression.getKey(), boundExpression.getValue());
        }
    }

    private static final class DataExtractorImpl<K, V> implements DataExtractor<K, V> {

        private final Set<KeySelector<K>> keySelectors;
        private final Set<ValueSelector<V>> valueSelectors;
        private final Set<ConstantSelector> metaSelectors;

        private final Schema schema;

        DataExtractorImpl(DataExtractorBuilder<K, V> builder) {
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
        public DataContainer extractData(KafkaRecord<K, V> record) throws ValueException {
            Set<Data> s = new HashSet<>();
            for (KeySelector<K> selector : keySelectors) {
                s.add(selector.extractKey(record));
            }
            for (ValueSelector<V> selector : valueSelectors) {
                s.add(selector.extractValue(record));
            }
            for (ConstantSelector selector : metaSelectors) {
                s.add(selector.extract(record));
            }
            return new DefaultValuesContainer(this, s, Collections.emptyMap());
        }

        @Override
        public DataContainer extractData2_0(KafkaRecord<K, V> record) throws ValueException {
            Map<String, String> map = new HashMap<>();
            for (KeySelector<K> selector : keySelectors) {
                Data data = selector.extractKey(record);
                map.put(data.name(), data.text());
            }
            for (ValueSelector<V> selector : valueSelectors) {
                Data data = selector.extractValue(record);
                map.put(data.name(), data.text());
            }
            for (ConstantSelector selector : metaSelectors) {
                Data data = selector.extract(record);
                map.put(data.name(), data.text());
            }

            return new DefaultValuesContainer(this, Collections.emptySet(), map);
        }

        @Override
        public DataContainer extractDataOld1_0(KafkaRecord<K, V> record) throws ValueException {
            return new DefaultValuesContainer(
                    this,
                    Stream.of(
                                    keySelectors.stream().map(k -> k.extractKey(record)),
                                    valueSelectors.stream().map(v -> v.extractValue(record)),
                                    metaSelectors.stream().map(m -> m.extract(record)))
                            .flatMap(identity())
                            .collect(toSet()),
                    Collections.emptyMap());
        }

        @Override
        public DataContainer extractDataOld1_1(KafkaRecord<K, V> record) throws ValueException {
            Stream<Data> keys = keySelectors.stream().map(k -> k.extractKey(record));
            Stream<Data> values = valueSelectors.stream().map(v -> v.extractValue(record));
            Stream<Data> meta = metaSelectors.stream().map(m -> m.extract(record));
            Stream<Data> s = Stream.concat(keys, values);
            Stream<Data> s1 = Stream.concat(s, meta);

            return new DefaultValuesContainer(this, s1.collect(toSet()), Collections.emptyMap());
        }

        @Override
        public int hashCode() {
            return Objects.hash(keySelectors, valueSelectors, metaSelectors, schema);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DataExtractorImpl<?, ?> other
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

    private DataExtractorSupport() {}
}
