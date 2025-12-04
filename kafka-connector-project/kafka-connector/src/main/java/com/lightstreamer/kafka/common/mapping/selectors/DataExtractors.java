
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.OFFSET;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.PARTITION;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TIMESTAMP;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TOPIC;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

public class DataExtractors {

    private interface DataExtractor<K, V> {

        Data extract(KafkaRecord<K, V> record) throws ValueException;
    }

    private interface MapExtractor<K, V> {

        void extract(KafkaRecord<K, V> record, Map<String, String> target) throws ValueException;
    }

    private static class ExtractorsProvider<K, V> {

        private final KeySelectorSupplier<K> keySelectorSupplier;
        private final ValueSelectorSupplier<V> valueSelectorSupplier;
        private final HeadersSelectorSupplier headersSelectorSupplier =
                new HeadersSelectorSupplier();
        private final ConstantSelectorSupplier constantSelectorSupplier =
                new ConstantSelectorSupplier(OFFSET, PARTITION, TIMESTAMP, TOPIC);

        ExtractorsProvider(
                KeySelectorSupplier<K> keySelectorSupplier,
                ValueSelectorSupplier<V> valueSelectorSupplier) {
            this.keySelectorSupplier = keySelectorSupplier;
            this.valueSelectorSupplier = valueSelectorSupplier;
        }

        KeySelector<K> newKeySelector(ExtractionExpression expression) throws ExtractionException {
            return keySelectorSupplier.newSelector(expression);
        }

        ValueSelector<V> newValueSelector(ExtractionExpression expression)
                throws ExtractionException {
            return valueSelectorSupplier.newSelector(expression);
        }

        GenericSelector newHeadersSelector(ExtractionExpression expression)
                throws ExtractionException {
            return headersSelectorSupplier.newSelector(expression);
        }

        ConstantSelector newConstantSelector(ExtractionExpression expression)
                throws ExtractionException {
            return constantSelectorSupplier.newSelector(expression);
        }

        private DataExtractor<K, V> createDataExtractor(
                String param, ExtractionExpression expression, boolean mapNonScalars)
                throws ExtractionException {
            return switch (expression.constant()) {
                case KEY -> {
                    KeySelector<K> keySelector = newKeySelector(expression);
                    yield record -> keySelector.extractKey(param, record, !mapNonScalars);
                }
                case VALUE -> {
                    ValueSelector<V> valueSelector = newValueSelector(expression);
                    yield record -> valueSelector.extractValue(param, record, !mapNonScalars);
                }
                case HEADERS -> {
                    GenericSelector headerSelector = newHeadersSelector(expression);
                    yield record -> headerSelector.extract(param, record, !mapNonScalars);
                }
                default -> {
                    ConstantSelector constantSelector = newConstantSelector(expression);
                    yield record -> constantSelector.extract(param, record, !mapNonScalars);
                }
            };
        }

        private MapExtractor<K, V> createMapExtractor(ExtractionExpression expression)
                throws ExtractionException {
            return switch (expression.constant()) {
                case KEY -> {
                    KeySelector<K> keySelector = newKeySelector(expression);
                    yield (record, target) -> keySelector.extractKeyInto(record, target);
                }
                case VALUE -> {
                    ValueSelector<V> valueSelector = newValueSelector(expression);
                    yield (record, target) -> valueSelector.extractValueInto(record, target);
                }
                case HEADERS -> {
                    GenericSelector headerSelector = newHeadersSelector(expression);
                    yield (record, target) -> headerSelector.extractInto(record, target);
                }
                default -> {
                    ConstantSelector constantSelector = newConstantSelector(expression);
                    yield (record, target) -> constantSelector.extractInto(record, target);
                }
            };
        }

        static <K, V> ExtractorsProvider<K, V> create(KeyValueSelectorSuppliers<K, V> sSuppliers) {
            return new ExtractorsProvider<>(
                    sSuppliers.keySelectorSupplier(), sSuppliers.valueSelectorSupplier());
        }
    }

    static class CanonicalItemExtractorImpl<K, V> implements CanonicalItemExtractor<K, V> {

        private final DataExtractor<K, V>[] extractors;
        private final Schema schema;
        private Function<KafkaRecord<K, V>, String> canonicalItemGenerator;

        @SuppressWarnings("unchecked")
        CanonicalItemExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                String schemaName,
                Map<String, ExtractionExpression> expressions)
                throws ExtractionException {

            ExtractorsProvider<K, V> provider = ExtractorsProvider.create(sSuppliers);

            Set<String> schemaKeys = new HashSet<>();
            this.extractors = (DataExtractor<K, V>[]) new DataExtractor[expressions.size()];
            Map<String, ExtractionExpression> sortedExpressions = new TreeMap<>(expressions);
            int index = 0;
            for (Map.Entry<String, ExtractionExpression> boundExpression :
                    sortedExpressions.entrySet()) {
                String param = boundExpression.getKey();
                schemaKeys.add(param);
                this.extractors[index++] =
                        provider.createDataExtractor(param, boundExpression.getValue(), false);
            }

            this.schema = Schema.from(schemaName, schemaKeys);
            switch (this.extractors.length) {
                case 0 -> this.canonicalItemGenerator = record -> schemaName;
                case 1 ->
                        this.canonicalItemGenerator =
                                record ->
                                        Data.buildItemNameSingle(
                                                this.extractors[0].extract(record), schemaName);
                default ->
                        this.canonicalItemGenerator =
                                record ->
                                        Data.buildItemName(
                                                this.extractDataArray(record), schemaName);
            }
        }

        private Data[] extractDataArray(KafkaRecord<K, V> record) {
            Data[] data = new Data[this.extractors.length];
            for (int i = 0; i < this.extractors.length; i++) {
                data[i] = this.extractors[i].extract(record);
            }
            return data;
        }

        @Override
        public String extractCanonicalItem(KafkaRecord<K, V> record) throws ValueException {
            return canonicalItemGenerator.apply(record);
        }

        @Override
        public Schema schema() {
            return schema;
        }
    }

    static class StaticFieldsExtractorImpl<K, V> implements FieldsExtractor<K, V> {

        private final DataExtractor<K, V>[] extractors;
        private final boolean skipOnFailure;
        private final boolean mapNonScalars;
        private Set<String> fieldNames;

        @SuppressWarnings("unchecked")
        StaticFieldsExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                Map<String, ExtractionExpression> expressions,
                boolean skipOnFailure,
                boolean mapNonScalars)
                throws ExtractionException {

            this.skipOnFailure = skipOnFailure;
            this.mapNonScalars = mapNonScalars;
            this.extractors = (DataExtractor<K, V>[]) new DataExtractor[expressions.size()];
            ExtractorsProvider<K, V> provider = ExtractorsProvider.create(sSuppliers);

            Set<String> fields = new HashSet<>();
            int index = 0;
            for (Map.Entry<String, ExtractionExpression> boundExpression : expressions.entrySet()) {
                String fieldName = boundExpression.getKey();
                this.extractors[index++] =
                        provider.createDataExtractor(
                                fieldName, boundExpression.getValue(), mapNonScalars);
                fields.add(fieldName);
            }
            this.fieldNames = Collections.unmodifiableSet(fields);
        }

        @Override
        public void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
                throws ValueException {
            for (int i = 0; i < this.extractors.length; i++) {
                try {
                    Data data = this.extractors[i].extract(record);
                    targetMap.put(data.name(), data.text());
                } catch (ValueException ve) {
                    if (!skipOnFailure) {
                        throw ve;
                    }
                }
            }
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
        public Set<String> mappedFields() {
            return fieldNames;
        }
    }

    static class DynamicFieldsExtractorImpl<K, V> implements FieldsExtractor<K, V> {

        private final MapExtractor<K, V>[] mapExtractors;
        private final boolean skipOnFailure;

        @SuppressWarnings("unchecked")
        DynamicFieldsExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                Map<String, ExtractionExpression> expressions,
                boolean skipOnFailure)
                throws ExtractionException {

            this.skipOnFailure = skipOnFailure;
            this.mapExtractors = (MapExtractor<K, V>[]) new MapExtractor[expressions.size()];
            ExtractorsProvider<K, V> provider = ExtractorsProvider.create(sSuppliers);

            int index = 0;
            for (ExtractionExpression boundExpression : expressions.values()) {
                this.mapExtractors[index++] = provider.createMapExtractor(boundExpression);
            }
        }

        public void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
                throws ValueException {
            for (int i = 0; i < this.mapExtractors.length; i++) {
                try {
                    this.mapExtractors[i].extract(record, targetMap);
                } catch (ValueException ve) {
                    if (!skipOnFailure) {
                        throw ve;
                    }
                }
            }
        }

        @Override
        public boolean skipOnFailure() {
            return skipOnFailure;
        }

        @Override
        public boolean mapNonScalars() {
            return true;
        }

        @Override
        public Set<String> mappedFields() {
            return Collections.emptySet();
        }
    }

    static class ComposedFieldExtractor<K, V> implements FieldsExtractor<K, V> {

        private final List<FieldsExtractor<K, V>> extractors;

        ComposedFieldExtractor(List<FieldsExtractor<K, V>> extractors) {
            this.extractors = extractors;
        }

        @Override
        public void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
                throws ValueException {
            for (int i = 0; i < this.extractors.size(); i++) {
                this.extractors.get(i).extractIntoMap(record, targetMap);
            }
        }

        @Override
        public boolean skipOnFailure() {
            boolean skipOnFailure = true;
            for (int i = 0; i < this.extractors.size() && skipOnFailure; i++) {
                skipOnFailure = this.extractors.get(i).skipOnFailure();
            }
            return skipOnFailure;
        }

        @Override
        public boolean mapNonScalars() {
            boolean mapNonScalars = true;
            for (int i = 0; i < this.extractors.size() && mapNonScalars; i++) {
                mapNonScalars = this.extractors.get(i).mapNonScalars();
            }
            return mapNonScalars;
        }

        @Override
        public Set<String> mappedFields() {
            Set<String> fieldNames = new HashSet<>();
            for (int i = 0; i < this.extractors.size(); i++) {
                fieldNames.addAll(this.extractors.get(i).mappedFields());
            }
            return fieldNames;
        }
    }

    public static <K, V> FieldsExtractor<K, V> staticFieldsExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {
        return new StaticFieldsExtractorImpl<>(
                sSuppliers, expressions, skipOnFailure, mapNonScalars);
    }

    public static <K, V> FieldsExtractor<K, V> dynamicFieldsExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure)
            throws ExtractionException {
        return new DynamicFieldsExtractorImpl<>(sSuppliers, expressions, skipOnFailure);
    }

    public static <K, V> FieldsExtractor<K, V> composedFieldsExtractor(
            List<FieldsExtractor<K, V>> extractors) {
        return new ComposedFieldExtractor<>(extractors);
    }

    private DataExtractors() {}
}
