
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
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
                ConstantSelectorSupplier.makeSelectorSupplier(OFFSET, PARTITION, TIMESTAMP, TOPIC);

        ExtractorsProvider(
                KeySelectorSupplier<K> keySelectorSupplier,
                ValueSelectorSupplier<V> valueSelectorSupplier) {
            this.keySelectorSupplier = keySelectorSupplier;
            this.valueSelectorSupplier = valueSelectorSupplier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    keySelectorSupplier.evaluatorType(), valueSelectorSupplier.evaluatorType());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            if (obj instanceof ExtractorsProvider<?, ?> other) {
                return Objects.equals(
                                this.keySelectorSupplier.evaluatorType(),
                                other.keySelectorSupplier.evaluatorType())
                        && Objects.equals(
                                this.valueSelectorSupplier.evaluatorType(),
                                other.valueSelectorSupplier.evaluatorType());
            }
            return false;
        }

        KeySelector<K> newKeySelector(ExtractionExpression expression) throws ExtractionException {
            return keySelectorSupplier.newSelector(expression);
        }

        ValueSelector<V> newValueSelector(ExtractionExpression expression)
                throws ExtractionException {
            return valueSelectorSupplier.newSelector(expression);
        }

        HeadersSelector newHeadersSelector(ExtractionExpression expression)
                throws ExtractionException {
            return headersSelectorSupplier.newSelector(expression);
        }

        GenericSelector newConstantSelector(ExtractionExpression expression)
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
                    HeadersSelector headerSelector = newHeadersSelector(expression);
                    yield record -> headerSelector.extract(param, record, !mapNonScalars);
                }
                default -> {
                    GenericSelector constantSelector = newConstantSelector(expression);
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
                    HeadersSelector headerSelector = newHeadersSelector(expression);
                    yield (record, target) -> headerSelector.extractInto(record, target);
                }
                default ->
                        // GenericSelector constantSelector = newConstantSelector(expression);
                        // yield (record, target) -> constantSelector.extractInto(record, target);
                        throw new IllegalArgumentException(
                                "Cannot handle dynamic extraction from the constant expression ["
                                        + expression.constant()
                                        + "]");
            };
        }

        static <K, V> ExtractorsProvider<K, V> create(KeyValueSelectorSuppliers<K, V> sSuppliers) {
            return new ExtractorsProvider<>(
                    sSuppliers.keySelectorSupplier(), sSuppliers.valueSelectorSupplier());
        }
    }

    static class CanonicalItemExtractorImpl<K, V> implements CanonicalItemExtractor<K, V> {

        private final ExtractorsProvider<K, V> provider;
        private final TemplateExpression template;
        private final DataExtractor<K, V>[] extractors;
        private final Function<KafkaRecord<K, V>, String> canonicalItemGenerator;

        @SuppressWarnings("unchecked")
        CanonicalItemExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers, TemplateExpression template)
                throws ExtractionException {

            this.provider = ExtractorsProvider.create(sSuppliers);
            this.template = template;
            this.extractors = (DataExtractor<K, V>[]) new DataExtractor[template.params().size()];

            int index = 0;
            for (Map.Entry<String, ExtractionExpression> expression :
                    template.params().entrySet()) {
                this.extractors[index++] =
                        provider.createDataExtractor(
                                expression.getKey(), expression.getValue(), false);
            }

            Schema schema = template.schema();
            switch (this.extractors.length) {
                case 0 -> this.canonicalItemGenerator = record -> schema.name();
                case 1 ->
                        this.canonicalItemGenerator =
                                record ->
                                        Data.buildItemNameSingle(
                                                this.extractors[0].extract(record), schema.name());
                default ->
                        this.canonicalItemGenerator =
                                record ->
                                        Data.buildItemName(
                                                this.extractDataArray(record), schema.name());
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
        public int hashCode() {
            return Objects.hash(provider, template);
        }

        public boolean equals(Object obj) {
            if (this == obj) return true;

            if (obj instanceof CanonicalItemExtractorImpl<?, ?> other) {
                return Objects.equals(this.template, other.template)
                        && Objects.equals(this.provider, other.provider);
            }
            return false;
        }

        @Override
        public Schema schema() {
            return template.schema();
        }

        @Override
        public String extractCanonicalItem(KafkaRecord<K, V> record) throws ValueException {
            return canonicalItemGenerator.apply(record);
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
                Collection<ExtractionExpression> expressions,
                boolean skipOnFailure)
                throws ExtractionException {

            this.skipOnFailure = skipOnFailure;
            this.mapExtractors = (MapExtractor<K, V>[]) new MapExtractor[expressions.size()];
            ExtractorsProvider<K, V> provider = ExtractorsProvider.create(sSuppliers);

            int index = 0;
            for (ExtractionExpression boundExpression : expressions) {
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

    /**
     * A composite {@link FieldsExtractor} that applies multiple field extractors in sequence to a
     * Kafka record.
     *
     * <p>This class enables the combination of multiple extraction strategies, where each extractor
     * in the composition contributes to building the final field map. Extractors are applied in the
     * order they are provided, and each extractor writes its extracted fields into the same target
     * map.
     *
     * <p><b>Field Override Behavior:</b> When multiple extractors produce values for the same field
     * name, later extractors will override values set by earlier ones.
     *
     * <p><b>Error Handling:</b> The {@link #skipOnFailure()} behavior is determined by evaluating
     * all composed extractors. The composite extractor will skip on failure only if all individual
     * extractors indicate they should skip on failure.
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
    static class ComposedFieldExtractor<K, V> implements FieldsExtractor<K, V> {

        private final List<FieldsExtractor<K, V>> extractors;

        /**
         * Constructs a ComposedFieldExtractor with the specified list of field extractors.
         *
         * @param extractors the list of {@link FieldsExtractor} instances to be composed. Must not
         *     be {@code null} or empty. The order of extractors in the list determines the
         *     application order, which affects the final field values when multiple extractors
         *     target the same field names.
         */
        ComposedFieldExtractor(List<FieldsExtractor<K, V>> extractors) {
            if (extractors == null || extractors.isEmpty()) {
                throw new IllegalArgumentException("Extractors list must not be null or empty");
            }
            if (extractors.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException(
                        "Extractors list must not contain null elements");
            }
            this.extractors = extractors;
        }

        @Override
        public void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
                throws ValueException {
            for (int i = 0; i < this.extractors.size(); i++) {
                // The last extractor may override fields extracted by previous ones
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

    public static <K, V> CanonicalItemExtractor<K, V> canonicalItemExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, TemplateExpression template)
            throws ExtractionException {
        return new CanonicalItemExtractorImpl<>(sSuppliers, template);
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
            Collection<ExtractionExpression> expressions,
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
