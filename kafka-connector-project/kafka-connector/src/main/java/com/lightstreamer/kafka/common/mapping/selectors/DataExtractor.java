
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

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

import java.util.Collections;
import java.util.Map;

/**
 * The {@code DataExtractor} interface defines the core data transformation operations for
 * converting Kafka records into Lightstreamer-compatible formats. It serves as the fundamental
 * building block used by {@link com.lightstreamer.kafka.common.mapping.RecordMapper} to perform
 * template expansion and field extraction.
 *
 * <p>DataExtractor implementations handle two primary transformation operations:
 *
 * <ul>
 *   <li><strong>Canonical item name generation:</strong> Through {@link #extractAsCanonicalItem}
 *       for subscriber routing and template expansion
 *   <li><strong>Field data extraction:</strong> Through {@link #extractAsMap} for structured data
 *       updates to Lightstreamer clients
 * </ul>
 *
 * <p>The interface supports both template-based extraction (for generating dynamic item names) and
 * field-based extraction (for producing structured update data), enabling flexible transformation
 * of Kafka records into Lightstreamer's item-based architecture.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public interface DataExtractor<K, V> {

    /**
     * Extracts structured field data from a Kafka record for Lightstreamer client updates. This
     * method performs the field extraction operation used by {@link
     * com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord#fieldsMap()} to provide
     * update payloads to subscribers.
     *
     * <p>The extraction process transforms record data into a structured format suitable for
     * Lightstreamer's field-based update model, where field names correspond to the configured
     * schema and values are converted to string representations for client consumption.
     *
     * <p><strong>Processing behavior:</strong>
     *
     * <ul>
     *   <li>Evaluates configured field expressions against the record's data
     *   <li>Performs type conversion to string representations as needed
     *   <li>Applies schema-based field mapping according to the configured structure
     *   <li>Handles missing or invalid fields based on the {@link #skipOnFailure()} setting
     * </ul>
     *
     * <p><strong>Example:</strong> For a stock record, might extract: {@code {"symbol": "AAPL",
     * "price": "150.25", "timestamp": "2024-01-15T10:30:00Z"}}
     *
     * @param record the Kafka record containing the key, value, headers, and metadata to extract
     *     field data from
     * @return a map of field names to extracted string values representing the structured data;
     *     never null but may be empty for no-op extractors
     * @throws ValueException if field extraction fails due to data format issues, type conversion
     *     errors, or if the record contains data that cannot be properly processed by the
     *     configured field expressions
     */
    Map<String, String> extractAsMap(KafkaRecord<K, V> record) throws ValueException;

    /**
     * Extracts and returns the canonical item name from the given Kafka record through template
     * expansion and data transformation.
     *
     * <p>This method serves as a core component in the Kafka-to-Lightstreamer mapping process,
     * evaluating configured templates against the provided record to generate canonical item
     * identifiers. These identifiers are used by {@link
     * com.lightstreamer.kafka.common.mapping.RecordMapper#map(KafkaRecord)} to determine routing
     * targets for Lightstreamer subscribers.
     *
     * <p><strong>Processing behavior:</strong>
     *
     * <ul>
     *   <li>Evaluates template expressions against the record's key, value, headers, and metadata
     *   <li>Performs data extraction and type conversion as needed
     *   <li>Produces canonical item names in the format: {@code
     *       "prefix-[param1=value1,param2=value2]"}
     *   <li>Returns deterministic, consistent identifiers for identical record content
     * </ul>
     *
     * <p><strong>Example:</strong> For a record with value {@code {"symbol": "AAPL", "price":
     * 150.25}} and template {@code "stock-#{symbol=VALUE.symbol}"}, this method returns {@code
     * "stock-[symbol=AAPL]"}.
     *
     * @param record the Kafka record containing the key, value, headers, and metadata to extract
     *     canonical item identifier from
     * @return the canonical item name as a String representation following the format
     *     "prefix-[param=value,...]"; never null but may be empty for no-op extractors
     * @throws ValueException if template expansion fails due to data extraction issues, type
     *     conversion errors, or if the record contains data that cannot be properly processed by
     *     the configured template expressions
     */
    String extractAsCanonicalItem(KafkaRecord<K, V> record) throws ValueException;

    /**
     * Retrieves the schema associated with this data extractor, defining the structure and field
     * names for data extraction operations. The schema determines which fields are available for
     * extraction in {@link #extractAsMap} and how they are named in the resulting field map.
     *
     * <p>The schema serves as the contract between the extractor configuration and the expected
     * output structure, ensuring consistent field naming and data organization across different
     * records processed by this extractor.
     *
     * @return the {@link Schema} object representing the structure and field definitions for this
     *     extractor; never {@code null}
     */
    Schema schema();

    /**
     * Determines whether field extraction operations should be skipped when individual field
     * extraction failures occur. This setting affects the behavior of {@link #extractAsMap} when
     * processing records with missing or invalid field data.
     *
     * <p>When {@code true}, field extraction failures are silently ignored and the problematic
     * fields are omitted from the result map, allowing successful processing to continue. When
     * {@code false}, field extraction failures cause the entire extraction operation to fail with a
     * {@code ValueException}.
     *
     * @return {@code true} if field extraction should continue despite individual field failures,
     *     {@code false} to fail fast on any field extraction error
     */
    default boolean skipOnFailure() {
        return false;
    }

    /**
     * Determines whether non-scalar values (such as arrays, objects, or nested structures) should
     * be included in field extraction operations performed by {@link #extractAsMap}. This setting
     * controls how complex data types are handled during the transformation process.
     *
     * <p>When {@code true}, non-scalar values are converted to string representations (typically
     * JSON format) and included in the extracted field map. When {@code false}, only scalar values
     * (strings, numbers, booleans) are extracted, and complex structures are ignored.
     *
     * @return {@code true} if complex data structures should be serialized and included in field
     *     extraction, {@code false} to extract only scalar values
     */
    default boolean mapNonScalars() {
        return false;
    }

    /**
     * Creates a DataExtractor with fully configurable field extraction expressions and processing
     * options. This is the most flexible factory method allowing complete customization of both
     * template expansion and field extraction behavior.
     *
     * @param sSuppliers the key-value selector suppliers for accessing record components
     * @param schemaName the name of the schema defining the extraction structure
     * @param expressions a map of field names to extraction expressions for data transformation
     * @param skipOnFailure whether to skip individual field extraction failures rather than failing
     *     the entire operation
     * @param mapNonScalars whether to include non-scalar values in field extraction
     * @return a configured DataExtractor instance for the specified parameters
     * @throws ExtractionException if the extractor configuration is invalid or cannot be created
     */
    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            String schemaName,
            Map<String, ExtractionExpression> expressions,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {
        return DataExtractorSupport.extractor(
                sSuppliers, schemaName, expressions, skipOnFailure, mapNonScalars);
    }

    /**
     * Creates a DataExtractor from a template expression for canonical item name generation. This
     * factory method is typically used for template extractors in {@link
     * com.lightstreamer.kafka.common.mapping.RecordMapper} configurations.
     *
     * <p>The created extractor uses default settings: failure-sensitive processing and scalar-only
     * field extraction.
     *
     * @param sSuppliers the key-value selector suppliers for accessing record components
     * @param expression the template expression defining both schema name and parameter expressions
     * @return a configured DataExtractor instance for template-based extraction
     * @throws ExtractionException if the template expression is invalid or cannot be processed
     */
    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, TemplateExpression expression)
            throws ExtractionException {
        return extractor(sSuppliers, expression.prefix(), expression.params(), false, false);
    }

    /**
     * Creates a basic DataExtractor with only a schema name and no field extraction expressions.
     * This factory method creates extractors suitable for simple template expansion scenarios.
     *
     * <p>The created extractor uses default settings: no field expressions, failure-sensitive
     * processing, and scalar-only extraction.
     *
     * @param sSuppliers the key-value selector suppliers for accessing record components
     * @param schemaName the name of the schema defining the extraction structure
     * @return a configured DataExtractor instance with minimal field extraction
     * @throws ExtractionException if the schema name is invalid or the extractor cannot be created
     */
    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, String schemaName)
            throws ExtractionException {
        return extractor(sSuppliers, schemaName, Collections.emptyMap(), false, false);
    }
}
