
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

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

class DataExtractorSupport {

    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> suppliers,
            String schema,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {
        return new DataExtractorImpl<>(
                suppliers, schema, expressions, skipOnFailure, mapNonScalars);
    }

    private static final class DataExtractorImpl<K, V> implements DataExtractor<K, V> {

        private static record WrapperSelectors<K, V>(
                KeySelectorSupplier<K> keySelectorSupplier,
                ValueSelectorSupplier<V> valueSelectorSupplier,
                ConstantSelectorSupplier constantSelectorSupplier,
                HeadersSelectorSupplier headersSelectorSupplier) {}

        private final Schema schema;
        private final boolean skipOnFailure;
        private final Function<KafkaRecord<K, V>, Data>[] singleValueExtractors;
        private final BiConsumer<KafkaRecord<K, V>, Map<String, String>>[] multiValueExtractors;
        private final boolean mapNonScalars;
        private Function<KafkaRecord<K, V>, String> extractAsCanonical;

        @SuppressWarnings("unchecked")
        DataExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                String schemaName,
                Map<String, ExtractionExpression> expressions,
                boolean skipOnFailure,
                boolean mapNonScalars)
                throws ExtractionException {

            this.skipOnFailure = skipOnFailure;
            this.mapNonScalars = mapNonScalars;
            HeadersSelectorSupplier headersSelectorSupplier = new HeadersSelectorSupplier();
            ConstantSelectorSupplier constantSelectorSupplier =
                    new ConstantSelectorSupplier(
                            Constant.OFFSET,
                            Constant.PARTITION,
                            Constant.TIMESTAMP,
                            Constant.TOPIC);

            WrapperSelectors<K, V> wrapperSelectors =
                    new WrapperSelectors<>(
                            sSuppliers.keySelectorSupplier(),
                            sSuppliers.valueSelectorSupplier(),
                            constantSelectorSupplier,
                            headersSelectorSupplier);

            Set<String> schemaKeys = new HashSet<>();
            this.singleValueExtractors =
                    (Function<KafkaRecord<K, V>, Data>[]) new Function[expressions.size()];
            this.multiValueExtractors =
                    (BiConsumer<KafkaRecord<K, V>, Map<String, String>>[])
                            new BiConsumer[expressions.size()];
            Map<String, ExtractionExpression> sortedExpressions = new TreeMap<>(expressions);
            int index = 0;
            for (Map.Entry<String, ExtractionExpression> boundExpression :
                    sortedExpressions.entrySet()) {
                String key = boundExpression.getKey();
                schemaKeys.add(key);
                this.singleValueExtractors[index] =
                        createSingleValueDataExtractor(
                                wrapperSelectors, key, boundExpression.getValue());
                this.multiValueExtractors[index] =
                        createMultiValueDataExtractor(
                                wrapperSelectors, key, boundExpression.getValue());
                index++;
            }

            this.schema = Schema.from(schemaName, schemaKeys);
            switch (this.singleValueExtractors.length) {
                case 0 -> this.extractAsCanonical = record -> schemaName;
                case 1 ->
                        this.extractAsCanonical =
                                record ->
                                        Data.buildItemNameSingle(
                                                this.singleValueExtractors[0].apply(record),
                                                schemaName);
                default ->
                        this.extractAsCanonical =
                                record ->
                                        Data.buildItemName(
                                                this.extractDataArray(record), schemaName);
            }
        }

        private Function<KafkaRecord<K, V>, Data> createSingleValueDataExtractor(
                WrapperSelectors<K, V> wrapperSelectors,
                String key,
                ExtractionExpression expression)
                throws ExtractionException {
            Function<KafkaRecord<K, V>, Data> dataExtractor =
                    switch (expression.constant()) {
                        case KEY -> {
                            KeySelector<K> keySelector =
                                    wrapperSelectors.keySelectorSupplier().newSelector(expression);
                            yield record -> keySelector.extractKey(key, record, !mapNonScalars);
                        }
                        case VALUE -> {
                            ValueSelector<V> valueSelector =
                                    wrapperSelectors
                                            .valueSelectorSupplier()
                                            .newSelector(expression);
                            yield record -> valueSelector.extractValue(key, record, !mapNonScalars);
                        }
                        case HEADERS -> {
                            GenericSelector headerSelector =
                                    wrapperSelectors
                                            .headersSelectorSupplier()
                                            .newSelector(expression);
                            yield record -> headerSelector.extract(key, record);
                        }
                        default -> {
                            GenericSelector constantSelector =
                                    wrapperSelectors
                                            .constantSelectorSupplier()
                                            .newSelector(expression);
                            yield record -> constantSelector.extract(key, record);
                        }
                    };
            return dataExtractor;
        }

        private BiConsumer<KafkaRecord<K, V>, Map<String, String>> createMultiValueDataExtractor(
                WrapperSelectors<K, V> wrapperSelectors,
                String key,
                ExtractionExpression expression)
                throws ExtractionException {
            BiConsumer<KafkaRecord<K, V>, Map<String, String>> dataExtractor =
                    switch (expression.constant()) {
                        case KEY -> {
                            KeySelector<K> keySelector =
                                    mkSelector(wrapperSelectors.keySelectorSupplier(), expression);
                            yield (record, target) -> keySelector.extractKeyInto(record, target);
                        }
                        case VALUE -> {
                            ValueSelector<V> valueSelector =
                                    mkSelector(
                                            wrapperSelectors.valueSelectorSupplier(), expression);
                            yield (record, target) ->
                                    valueSelector.extractValueInto(record, target);
                        }
                        case HEADERS -> {
                            GenericSelector headerSelector =
                                    mkSelector(
                                            wrapperSelectors.headersSelectorSupplier(), expression);
                            yield (record, target) -> headerSelector.extractInto(record, target);
                        }
                        default -> {
                            GenericSelector constantSelector =
                                    mkSelector(
                                            wrapperSelectors.constantSelectorSupplier(),
                                            expression);
                            yield (record, target) -> constantSelector.extractInto(record, target);
                        }
                    };
            return dataExtractor;
        }

        static <T extends Selector> T mkSelector(
                SelectorSupplier<T> selectorSupplier, ExtractionExpression expression)
                throws ExtractionException {
            return selectorSupplier.newSelector(expression);
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public boolean skipOnFailure() {
            return skipOnFailure;
        }

        @Override
        public boolean mapNonScalars() {
            return mapNonScalars;
        }

        @Override
        public Map<String, String> extractAsMap(KafkaRecord<K, V> record) throws ValueException {
            Map<String, String> values = new HashMap<>();
            for (int i = 0; i < this.multiValueExtractors.length; i++) {
                try {
                    multiValueExtractors[i].accept(record, values);
                } catch (ValueException ve) {
                    if (!skipOnFailure) {
                        throw ve;
                    }
                }
            }
            return values;
        }

        @Override
        public String extractAsCanonicalItem(KafkaRecord<K, V> record) throws ValueException {
            return extractAsCanonical.apply(record);
        }

        private Data[] extractDataArray(KafkaRecord<K, V> record) {
            Data[] data = new Data[this.singleValueExtractors.length];
            for (int i = 0; i < this.singleValueExtractors.length; i++) {
                data[i] = this.singleValueExtractors[i].apply(record);
            }
            return data;
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, skipOnFailure, mapNonScalars);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DataExtractorImpl<?, ?> other
                    && Objects.equals(schema, other.schema)
                    && Objects.equals(skipOnFailure, other.skipOnFailure)
                    && Objects.equals(mapNonScalars, other.mapNonScalars);
        }
    }

    private DataExtractorSupport() {}
}
