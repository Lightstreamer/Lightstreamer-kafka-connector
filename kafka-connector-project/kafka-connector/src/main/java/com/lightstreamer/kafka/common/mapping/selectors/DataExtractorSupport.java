
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

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DataExtractorSupport {

    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> suppliers,
            String schema,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure)
            throws ExtractionException {
        return new DataExtractorImpl<>(suppliers, schema, expressions, skipOnFailure);
    }

    private static final class DataExtractorImpl<K, V> implements DataExtractor<K, V> {

        private final Schema schema;
        private final WrapperSelectors<K, V> wrapperSelectors;
        private final boolean skipOnFailure;

        DataExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                String schemaName,
                Map<String, ExtractionExpression> expressions,
                boolean skipOnFailure)
                throws ExtractionException {

            this.wrapperSelectors = mkWrapperSelectors(sSuppliers, expressions);
            this.schema = mkSchema(schemaName);
            this.skipOnFailure = skipOnFailure;
        }

        @Override
        public Schema schema() {
            return schema;
        }

        public SchemaAndValues extractData(KafkaRecord<K, V> record) throws ValueException {
            Map<String, String> values = new HashMap<>();
            for (KeySelector<K> selector : wrapperSelectors.keySelectors()) {
                tryFill(() -> selector.extractKey(record), values);
            }
            for (ValueSelector<V> selector : wrapperSelectors.valueSelectors()) {
                tryFill(() -> selector.extractValue(record), values);
            }
            for (ConstantSelector selector : wrapperSelectors.metaSelectors()) {
                tryFill(() -> selector.extract(record), values);
            }

            return new DefaultSchemaAndValues(schema, values);
        }

        private void tryFill(Supplier<Data> supplier, Map<String, String> values) {
            try {
                Data data = supplier.get();
                values.put(data.name(), data.text());
            } catch (ValueException ve) {
                if (!skipOnFailure) {
                    throw ve;
                }
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(wrapperSelectors, schema);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DataExtractorImpl<?, ?> other
                    && Objects.equals(wrapperSelectors, other.wrapperSelectors)
                    && Objects.equals(schema, other.schema);
        }

        private Schema mkSchema(String schemaName) {
            Stream<String> keyNames = wrapperSelectors.keySelectors().stream().map(Selector::name);
            Stream<String> valueNames =
                    wrapperSelectors.valueSelectors().stream().map(Selector::name);
            Stream<String> metaNames =
                    wrapperSelectors.metaSelectors().stream().map(Selector::name);

            return Schema.from(
                    schemaName,
                    Stream.of(metaNames, keyNames, valueNames)
                            .flatMap(Function.identity())
                            .collect(Collectors.toSet()));
        }
    }

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

    private static record WrapperSelectors<K, V>(
            Set<KeySelector<K>> keySelectors,
            Set<ValueSelector<V>> valueSelectors,
            Set<ConstantSelector> metaSelectors) {

        WrapperSelectors() {
            this(new HashSet<>(), new HashSet<>(), new HashSet<>());
        }
    }

    private static <K, V> WrapperSelectors<K, V> mkWrapperSelectors(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        WrapperSelectors<K, V> ws = new WrapperSelectors<>();
        Appender<KeySelector<K>> kFiller =
                new Appender<>(ws.keySelectors(), sSuppliers.keySelectorSupplier());
        Appender<ValueSelector<V>> vFiller =
                new Appender<>(ws.valueSelectors(), sSuppliers.valueSelectorSupplier());
        Appender<ConstantSelector> mFiller =
                new Appender<>(
                        ws.metaSelectors(),
                        new ConstantSelectorSupplier(
                                Constant.OFFSET,
                                Constant.PARTITION,
                                Constant.TIMESTAMP,
                                Constant.TOPIC));

        for (Map.Entry<String, ExtractionExpression> boundExpression : expressions.entrySet()) {
            Constant root = boundExpression.getValue().constant();
            Appender<?> filler =
                    switch (root) {
                        case KEY -> kFiller;
                        case VALUE -> vFiller;
                        default -> mFiller;
                    };
            filler.append(boundExpression.getKey(), boundExpression.getValue());
        }
        return ws;
    }

    private DataExtractorSupport() {}
}
