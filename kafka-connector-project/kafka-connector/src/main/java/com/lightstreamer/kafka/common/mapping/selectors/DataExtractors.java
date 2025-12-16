
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
import java.util.function.Predicate;

/**
 * Factory class for creating data extraction components that transform Kafka records into
 * Lightstreamer-compatible formats. This class consolidates all data extraction functionality into
 * a unified, type-safe API.
 *
 * <p>The {@code DataExtractors} class provides factory methods for creating specialized extractor
 * implementations that handle two primary transformation operations:
 *
 * <ul>
 *   <li><strong>Canonical item name generation:</strong> Through {@link CanonicalItemExtractor} for
 *       subscriber routing and template expansion
 *   <li><strong>Field data extraction:</strong> Through {@link FieldsExtractor} for mapping Kafka
 *       record content to Lightstreamer field names
 * </ul>
 *
 * <h2>Extractor Types</h2>
 *
 * <p>This class supports multiple extraction strategies to accommodate different use cases:
 *
 * <ul>
 *   <li><strong>Named field extraction:</strong> Via {@link #namedFieldsExtractor} for predefined
 *       field mappings where field names are known at configuration time
 *   <li><strong>Discovered field extraction:</strong> Via {@link #discoveredFieldsExtractor} for
 *       runtime field discovery where field names are determined from the record content
 *   <li><strong>Composed extraction:</strong> Via {@link #composedFieldsExtractor} for combining
 *       multiple extraction strategies in a layered approach
 *   <li><strong>Canonical item extraction:</strong> Via {@link #canonicalItemExtractor} for
 *       template-based generation of Lightstreamer item identifiers
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Create a canonical item extractor for routing
 * TemplateExpression template = // ... parsed template
 * CanonicalItemExtractor<String, JsonNode> itemExtractor =
 *     DataExtractors.canonicalItemExtractor(selectorSuppliers, template);
 *
 * // Create a named fields extractor for known fields
 * Map<String, ExtractionExpression> fieldMappings = // ... field definitions
 * FieldsExtractor<String, JsonNode> fieldsExtractor =
 *     DataExtractors.namedFieldsExtractor(selectorSuppliers, fieldMappings, false, false);
 *
 * // Use extractors with Kafka records
 * String itemName = itemExtractor.extractCanonicalItem(record);
 * Map<String, String> fields = fieldsExtractor.extractMap(record);
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>All extractor instances created by this factory are immutable and thread-safe once
 * constructed. Multiple threads can safely invoke extraction operations on the same extractor
 * instance concurrently.
 *
 * @see CanonicalItemExtractor
 * @see FieldsExtractor
 * @see com.lightstreamer.kafka.common.mapping.RecordMapper
 */
public class DataExtractors {

    /**
     * Internal interface for extracting a single {@link Data} element from a Kafka record. Used for
     * extracting individual field values or template parameters.
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
    private interface DataExtractor<K, V> {

        /**
         * Extracts data from the given Kafka record.
         *
         * @param record the Kafka record to extract data from
         * @return the extracted {@link Data} containing field name and value
         * @throws ValueException if extraction fails due to data format issues or type conversion
         *     errors
         */
        Data extract(KafkaRecord<K, V> record) throws ValueException;
    }

    /**
     * Internal interface for extracting multiple fields directly into a target map. Used for
     * dynamic field extraction where field names are determined at runtime from the record content.
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
    private interface MapExtractor<K, V> {

        /**
         * Extracts fields from the Kafka record and populates them into the target map.
         *
         * @param record the Kafka record to extract fields from
         * @param target the destination map to populate with extracted field name-value pairs
         * @throws ValueException if extraction fails due to data format issues or type conversion
         *     errors
         */
        void extract(KafkaRecord<K, V> record, Map<String, String> target) throws ValueException;
    }

    /**
     * Provider class that encapsulates selector suppliers for different parts of a Kafka record
     * (key, value, headers, metadata). This class serves as a factory for creating specific
     * extractor instances and ensures consistent selector creation across different extraction
     * strategies.
     *
     * <p>The provider maintains suppliers for:
     *
     * <ul>
     *   <li>Key selectors - for extracting data from record keys
     *   <li>Value selectors - for extracting data from record values
     *   <li>Headers selectors - for extracting data from record headers
     *   <li>Constant selectors - for extracting record metadata (offset, partition, timestamp,
     *       topic)
     * </ul>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
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

    /**
     * Implementation of {@link CanonicalItemExtractor} that generates canonical Lightstreamer item
     * names through template expansion and data extraction.
     *
     * <p>This class evaluates configured template expressions against Kafka records to produce
     * canonical item identifiers used for subscriber routing. The extraction process:
     *
     * <ul>
     *   <li>Evaluates template parameters against the record's key, value, headers, and metadata
     *   <li>Performs data extraction and type conversion as needed
     *   <li>Produces canonical item names in the format: {@code "prefix-[param1=value1,
     *       param2=value2]"}
     *   <li>Returns deterministic, consistent identifiers for identical record content
     * </ul>
     *
     * <p><strong>Example:</strong> For a record with value {@code {"symbol": "AAPL", "price":
     * 150.25}} and template {@code "stock-#{symbol=VALUE.symbol}"}, this produces {@code
     * "stock-[symbol=AAPL]"}.
     *
     * <p>The implementation is optimized based on the number of template parameters:
     *
     * <ul>
     *   <li>Zero parameters: Returns the schema name directly
     *   <li>One parameter: Uses optimized single-value extraction
     *   <li>Multiple parameters: Uses array-based extraction for all parameters
     * </ul>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
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

    /**
     * Implementation of {@link FieldsExtractor} for extracting named fields from Kafka records,
     * where field names are explicitly defined at configuration time.
     *
     * <p>This extractor is used when field names and their extraction expressions are known at
     * configuration time. It performs structured field extraction by:
     *
     * <ul>
     *   <li>Evaluating configured field expressions against the record's data
     *   <li>Performing type conversion to string representations
     *   <li>Applying schema-based field mapping according to the configured structure
     *   <li>Handling missing or invalid fields based on the {@link #skipOnFailure()} setting
     * </ul>
     *
     * <p><strong>Processing behavior:</strong>
     *
     * <ul>
     *   <li>Each field is extracted independently according to its expression
     *   <li>Field order is determined by the iteration order of the expressions map
     *   <li>If {@code skipOnFailure} is {@code true}, individual field failures are silently
     *       ignored
     *   <li>If {@code skipOnFailure} is {@code false}, any field failure aborts the entire
     *       extraction
     *   <li>Non-scalar value handling is controlled by the {@code mapNonScalars} flag
     * </ul>
     *
     * <p><strong>Example:</strong> For expressions {@code {"symbol": "VALUE.symbol", "price":
     * "VALUE.price"}}, extracts: {@code {"symbol": "AAPL", "price": "150.25"}}
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
    static class NamedFieldsExtractorImpl<K, V> implements FieldsExtractor<K, V> {

        private final DataExtractor<K, V>[] extractors;
        private final boolean skipOnFailure;
        private final boolean mapNonScalars;
        private Set<String> fieldNames;

        @SuppressWarnings("unchecked")
        NamedFieldsExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                Map<String, ExtractionExpression> expressions,
                boolean skipOnFailure,
                boolean mapNonScalars)
                throws ExtractionException {

            checkMap(expressions, "All expressions must be non-wildcard expressions");
            if (!areNamedExpressions(expressions.values())) {
                throw new IllegalArgumentException(
                        "All expressions must be non-wildcard expressions");
            }

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

        private static boolean areNamedExpressions(Collection<ExtractionExpression> expressions) {
            return expressions.stream()
                    .allMatch(Predicate.not(ExtractionExpression::isWildCardExpression));
        }

        private static void checkMap(Map<String, ?> map, String errorMessage) {
            if (map == null || map.isEmpty()) {
                throw new IllegalArgumentException(errorMessage);
            }

            if (map.keySet().stream().anyMatch(Objects::isNull)
                    || map.values().stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    /**
     * Implementation of {@link FieldsExtractor} for discovering and extracting fields at runtime,
     * where field names are determined from the record content rather than predefined in
     * configuration.
     *
     * <p>This extractor is used for scenarios where:
     *
     * <ul>
     *   <li>Field names cannot be known at configuration time
     *   <li>The record structure is variable or schema-less
     *   <li>All fields from a source (key, value, headers) should be extracted wholesale
     *   <li>Flattened extraction of nested structures is required
     * </ul>
     *
     * <p><strong>Processing behavior:</strong>
     *
     * <ul>
     *   <li>Each configured expression performs runtime field discovery
     *   <li>Field names are extracted from the record structure itself
     *   <li>All discovered fields are written directly to the target map
     *   <li>Multiple expressions may contribute different fields to the same result map
     *   <li>Later expressions may override fields set by earlier ones
     * </ul>
     *
     * <p><strong>Example:</strong> An expression {@code "VALUE.*"} might dynamically extract all
     * fields from a JSON value: {@code {"symbol": "AAPL", "price": "150.25", "volume": "1000000"}}
     *
     * <p><b>Note:</b> Dynamic extraction always maps non-scalar values (arrays, objects) as the
     * field structure itself may contain complex types. The {@link #mappedFields()} method returns
     * an empty set since field names are not statically known.
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     */
    static class DiscoveredFieldsExtractorImpl<K, V> implements FieldsExtractor<K, V> {

        private final MapExtractor<K, V>[] mapExtractors;
        private final boolean skipOnFailure;

        @SuppressWarnings("unchecked")
        DiscoveredFieldsExtractorImpl(
                KeyValueSelectorSuppliers<K, V> sSuppliers,
                Collection<ExtractionExpression> expressions,
                boolean skipOnFailure)
                throws ExtractionException {
            checkCollection(expressions, "All expressions must be wildcard expressions");
            if (!areWildcardExpressions(expressions)) {
                throw new IllegalArgumentException("All expressions must be wildcard expressions");
            }

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

        private static boolean areWildcardExpressions(
                Collection<ExtractionExpression> expressions) {
            return expressions.stream().allMatch(ExtractionExpression::isWildCardExpression);
        }

        private static void checkCollection(Collection<?> collection, String errorMessage) {
            if (collection == null || collection.isEmpty()) {
                throw new IllegalArgumentException(errorMessage);
            }

            if (collection.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException(errorMessage);
            }
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
    static class ComposedFieldsExtractorImpl<K, V> implements FieldsExtractor<K, V> {

        private final List<FieldsExtractor<K, V>> extractors;

        /**
         * Constructs a ComposedFieldsExtractorImpl with the specified list of field extractors.
         *
         * @param extractors the list of {@link FieldsExtractor} instances to be composed. Must not
         *     be {@code null} or empty. The order of extractors in the list determines the
         *     application order, which affects the final field values when multiple extractors
         *     target the same field names.
         */
        ComposedFieldsExtractorImpl(List<FieldsExtractor<K, V>> extractors) {
            if (extractors == null || extractors.isEmpty()) {
                throw new IllegalArgumentException("Extractors list must not be null or empty");
            }
            if (extractors.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Extractors list must not be null or empty");
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

    /**
     * Creates a {@link CanonicalItemExtractor} for generating canonical Lightstreamer item names
     * through template expansion.
     *
     * <p>The returned extractor evaluates the provided template expression against Kafka records to
     * produce deterministic item identifiers used by {@link
     * com.lightstreamer.kafka.common.mapping.RecordMapper} for subscriber routing.
     *
     * <p><strong>Template format:</strong> Templates consist of a prefix and zero or more
     * parameters: {@code "prefix-#{param1=EXPRESSION1,param2=EXPRESSION2}"}
     *
     * <p><strong>Parameter ordering:</strong> Parameters are automatically sorted in alphanumeric
     * order by their parameter names, ensuring deterministic canonical item names regardless of the
     * order specified in the template definition.
     *
     * <p><strong>Example usage:</strong>
     *
     * <pre>{@code
     * // Template: "stock-#{symbol=VALUE.symbol,exchange=VALUE.exchange}"
     * CanonicalItemExtractor<String, JsonNode> extractor =
     *     DataExtractors.canonicalItemExtractor(suppliers, template);
     *
     * // For record with value {"symbol": "AAPL", "exchange": "NASDAQ", "price": 150.25}
     * String itemName = extractor.extractCanonicalItem(record);
     * // Result: "stock-[exchange=NASDAQ,symbol=AAPL]" (parameters sorted alphabetically)
     * }</pre>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     * @param sSuppliers the selector suppliers providing key and value extraction capabilities
     * @param template the template expression defining the item name structure and parameter
     *     bindings
     * @return a new {@link CanonicalItemExtractor} instance configured with the specified template
     * @throws ExtractionException if the template expressions cannot be compiled or contain invalid
     *     selector references
     */
    public static <K, V> CanonicalItemExtractor<K, V> canonicalItemExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, TemplateExpression template)
            throws ExtractionException {
        return new CanonicalItemExtractorImpl<>(sSuppliers, template);
    }

    /**
     * Creates a {@link FieldsExtractor} for extracting named fields from Kafka records, where field
     * names are explicitly defined at configuration time.
     *
     * <p>This factory method is used when field names and their extraction expressions are known at
     * configuration time. The extractor performs structured field extraction with configurable
     * error handling and non-scalar value mapping.
     *
     * <p><strong>Non-wildcard expression requirement:</strong> All provided expressions
     * <em>must</em> be non-wildcard expressions (e.g., {@code "VALUE.symbol"}, {@code
     * "KEY.userId"}). Wildcard expressions will cause an {@link IllegalArgumentException} to be
     * thrown. Use {@link #discoveredFieldsExtractor} for runtime field discovery with wildcard
     * expressions.
     *
     * <p><strong>Field mapping behavior:</strong>
     *
     * <ul>
     *   <li>Each entry in {@code expressions} defines a field name and its extraction expression
     *   <li>Fields are extracted independently according to their configured expressions
     *   <li>The resulting map contains exactly the configured field names (unless extraction fails)
     *   <li>Field order is determined by the iteration order of the expressions map
     * </ul>
     *
     * <p><strong>Example usage:</strong>
     *
     * <pre>{@code
     * Map<String, ExtractionExpression> fields = Map.of(
     *     "symbol", ExpressionParser.parse("VALUE.symbol"),
     *     "price", ExpressionParser.parse("VALUE.price"),
     *     "timestamp", ExpressionParser.parse("TIMESTAMP")
     * );
     *
     * FieldsExtractor<String, JsonNode> extractor =
     *     DataExtractors.namedFieldsExtractor(suppliers, fields, false, false);
     *
     * Map<String, String> result = extractor.extractMap(record);
     * // Result: {"symbol": "AAPL", "price": "150.25", "timestamp": "1638360000000"}
     * }</pre>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     * @param sSuppliers the selector suppliers providing key and value extraction capabilities
     * @param expressions a map of field names to non-wildcard extraction expressions defining the
     *     fields to extract and their sources
     * @param skipOnFailure if {@code true}, individual field extraction failures are silently
     *     ignored and the field is omitted from results; if {@code false}, any field failure causes
     *     the entire extraction to fail with a {@link ValueException}
     * @param mapNonScalars if {@code true}, non-scalar values (arrays, objects) are converted to
     *     their string representations; if {@code false}, non-scalar values cause extraction
     *     failures
     * @return a new {@link FieldsExtractor} instance configured for named field extraction
     * @throws IllegalArgumentException if {@code expressions} is {@code null}, empty, contains
     *     {@code null} elements, or contains any wildcard expressions
     * @throws ExtractionException if any extraction expression cannot be compiled or contains
     *     invalid selector references
     */
    public static <K, V> FieldsExtractor<K, V> namedFieldsExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {

        return new NamedFieldsExtractorImpl<>(
                sSuppliers, expressions, skipOnFailure, mapNonScalars);
    }

    /**
     * Creates a {@link FieldsExtractor} for discovering and extracting fields at runtime, where
     * field names are determined from the record content rather than predefined in configuration.
     *
     * <p>This factory method is used for scenarios where field names cannot be known at
     * configuration time, such as schema-less records, variable structures, or wholesale extraction
     * of nested objects.
     *
     * <p><strong>Wildcard expression requirement:</strong> All provided expressions <em>must</em>
     * be wildcard expressions (e.g., {@code "VALUE.*"}, {@code "KEY.*"}). Non-wildcard expressions
     * will cause an {@link IllegalArgumentException} to be thrown. Use {@link
     * #namedFieldsExtractor} for extracting specific named fields.
     *
     * <p><strong>Dynamic extraction behavior:</strong>
     *
     * <ul>
     *   <li>Field names are discovered at runtime from the record structure
     *   <li>Multiple expressions can contribute different fields to the result
     *   <li>Later expressions may override fields set by earlier ones
     *   <li>Non-scalar values are always mapped (converted to string representations)
     *   <li>The {@link FieldsExtractor#mappedFields()} method returns an empty set
     * </ul>
     *
     * <p><strong>Example usage:</strong>
     *
     * <pre>{@code
     * // Extract all fields from the value
     * Collection<ExtractionExpression> expressions = List.of(
     *     ExpressionParser.parse("VALUE.*")
     * );
     *
     * FieldsExtractor<String, JsonNode> extractor =
     *     DataExtractors.discoveredFieldsExtractor(suppliers, expressions, true);
     *
     * // For record with value {"symbol": "AAPL", "price": 150.25, "volume": 1000000}
     * Map<String, String> result = extractor.extractMap(record);
     * // Result: {"symbol": "AAPL", "price": "150.25", "volume": "1000000"}
     * }</pre>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     * @param sSuppliers the selector suppliers providing key and value extraction capabilities
     * @param expressions a collection of wildcard extraction expressions that perform runtime field
     *     discovery from the record
     * @param skipOnFailure if {@code true}, extraction failures are silently ignored and
     *     problematic fields are omitted; if {@code false}, any extraction failure causes the
     *     entire operation to fail with a {@link ValueException}
     * @return a new {@link FieldsExtractor} instance configured for dynamic field extraction
     * @throws IllegalArgumentException if {@code expressions} is {@code null}, empty, contains
     *     {@code null} elements, or contains any non-wildcard expressions
     * @throws ExtractionException if any extraction expression cannot be compiled or contains
     *     invalid selector references
     */
    public static <K, V> FieldsExtractor<K, V> discoveredFieldsExtractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            Collection<ExtractionExpression> expressions,
            boolean skipOnFailure)
            throws ExtractionException {
        return new DiscoveredFieldsExtractorImpl<>(sSuppliers, expressions, skipOnFailure);
    }

    /**
     * Creates a composite {@link FieldsExtractor} that combines multiple field extractors, applying
     * them in sequence to build a unified field map.
     *
     * <p>This factory method enables sophisticated extraction strategies by composing multiple
     * extractors together. Common use cases include:
     *
     * <ul>
     *   <li>Combining named field extraction with discovered extraction
     *   <li>Setting default field values with one extractor, then overriding selectively
     *   <li>Extracting from multiple sources (key, value, headers) with separate extractors
     *   <li>Applying layered transformation or fallback logic
     * </ul>
     *
     * <p><strong>Composition behavior:</strong>
     *
     * <ul>
     *   <li>Extractors are applied in the order provided
     *   <li>Each extractor writes to the same shared target map
     *   <li>Later extractors override earlier values for the same field name
     *   <li>The composite skips on failure only if <em>all</em> extractors skip on failure
     *   <li>The composite maps non-scalars only if <em>all</em> extractors map non-scalars
     *   <li>Mapped fields are the union of all extractors' mapped fields
     * </ul>
     *
     * <p><strong>Example usage:</strong>
     *
     * <pre>{@code
     * // Combine named metadata extraction with discovered value extraction
     * FieldsExtractor<String, JsonNode> metadataExtractor =
     *     DataExtractors.namedFieldsExtractor(suppliers, metadataFields, false, false);
     * FieldsExtractor<String, JsonNode> discoveredExtractor =
     *     DataExtractors.discoveredFieldsExtractor(suppliers, List.of(parse("VALUE.*")), true);
     *
     * FieldsExtractor<String, JsonNode> composed =
     *     DataExtractors.composedFieldsExtractor(List.of(metadataExtractor, discoveredExtractor));
     *
     * // Extracts metadata fields first, then adds all value fields
     * Map<String, String> result = composed.extractMap(record);
     * }</pre>
     *
     * @param <K> the type of the Kafka record key
     * @param <V> the type of the Kafka record value
     * @param extractors the list of {@link FieldsExtractor} instances to compose. Must not be
     *     {@code null}, empty, or contain {@code null} elements. Order determines application
     *     sequence and field override behavior.
     * @return a new composite {@link FieldsExtractor} that applies all provided extractors in
     *     sequence
     * @throws IllegalArgumentException if {@code extractors} is {@code null}, empty, or contains
     *     {@code null} elements
     */
    public static <K, V> FieldsExtractor<K, V> composedFieldsExtractor(
            List<FieldsExtractor<K, V>> extractors) {
        return new ComposedFieldsExtractorImpl<>(extractors);
    }

    private DataExtractors() {}
}
