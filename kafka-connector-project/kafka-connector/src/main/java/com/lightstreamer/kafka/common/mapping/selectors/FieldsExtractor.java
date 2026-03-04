
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

import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.util.Map;
import java.util.Set;

/**
 * Interface for extracting Lightstreamer field names and values from Kafka records.
 *
 * <p>A {@code FieldsExtractor} transforms Kafka record content into a map of field name-value pairs
 * suitable for Lightstreamer client updates. The extractor defines which parts of a Kafka record
 * are extracted, how they are mapped to field names, and how extraction failures are handled.
 *
 * <p>Implementations support two primary extraction modes:
 *
 * <ul>
 *   <li><strong>Named field extraction:</strong> Field names are predefined at configuration time,
 *       and the extractor produces a fixed set of fields according to configured expressions
 *   <li><strong>Discovered field extraction:</strong> Field names are determined at runtime from
 *       the record structure, enabling dynamic field discovery from schema-less or
 *       variable-structure records
 * </ul>
 *
 * <h2>Extraction Behavior</h2>
 *
 * <p>All extracted values are represented as strings. The extractor handles type conversion
 * automatically:
 *
 * <ul>
 *   <li><strong>Scalar values</strong> (strings, numbers, booleans, null) are converted to their
 *       string representation
 *   <li><strong>Non-scalar values</strong> (objects, arrays) are handled according to the {@link
 *       #mapNonScalars()} policy - either serialized to JSON strings or omitted
 * </ul>
 *
 * <p><strong>Error handling</strong> is controlled by the {@link #skipOnFailure()} policy:
 *
 * <ul>
 *   <li>When {@code true}, extraction failures are silently ignored and problematic fields are
 *       omitted from the result
 *   <li>When {@code false}, any extraction failure causes the entire operation to fail with a
 *       {@link ValueException}
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Create an extractor for specific fields
 * Map<String, ExtractionExpression> expressions = Map.of(
 *     "symbol", ExpressionParser.parse("VALUE.symbol"),
 *     "price", ExpressionParser.parse("VALUE.price"),
 *     "timestamp", ExpressionParser.parse("TIMESTAMP")
 * );
 *
 * FieldsExtractor<String, JsonNode> extractor =
 *     DataExtractors.namedFieldsExtractor(suppliers, expressions, false, false);
 *
 * // Extract fields from a record
 * Map<String, String> fields = extractor.extractMap(record);
 * // Result: {"symbol": "AAPL", "price": "150.25", "timestamp": "1702650000000"}
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Implementations are immutable and thread-safe. Multiple threads can safely invoke extraction
 * operations on the same extractor instance concurrently.
 *
 * @param <K> the type of the Kafka record key
 * @param <V> the type of the Kafka record value
 * @see DataExtractors#namedFieldsExtractor
 * @see DataExtractors#discoveredFieldsExtractor
 * @see DataExtractors#composedFieldsExtractor
 * @see CanonicalItemExtractor
 */
public interface FieldsExtractor<K, V> {

    /**
     * Extracts Lightstreamer fields from the given Kafka record into a new map.
     *
     * <p>This convenience method creates a new {@link java.util.HashMap} and delegates to {@link
     * #extractIntoMap(KafkaRecord, Map)} to populate it.
     *
     * @param record the Kafka record to extract fields from
     * @return a new map containing the extracted field name-value pairs
     * @throws ValueException if extraction fails and {@link #skipOnFailure()} returns {@code false}
     */
    default Map<String, String> extractMap(KafkaRecord<K, V> record) throws ValueException {
        Map<String, String> values = new java.util.HashMap<>();
        extractIntoMap(record, values);
        return values;
    }

    /**
     * Extracts Lightstreamer fields from the given Kafka record and populates them into the target
     * map.
     *
     * <p>This method performs the core extraction logic, evaluating configured expressions against
     * the record and writing results to the provided map. Existing entries in the target map are
     * preserved unless overwritten by extracted fields with the same name.
     *
     * <p><strong>Extraction behavior:</strong>
     *
     * <ul>
     *   <li>For <strong>named field extraction</strong>, only the predefined field names are
     *       extracted and added to the map
     *   <li>For <strong>discovered field extraction</strong>, field names are determined from the
     *       record structure and added dynamically
     *   <li>Field values are always represented as strings, with automatic type conversion
     *   <li>Non-scalar values are handled according to the {@link #mapNonScalars()} policy
     *   <li>Extraction failures are handled according to the {@link #skipOnFailure()} policy
     * </ul>
     *
     * @param record the Kafka record to extract fields from
     * @param targetMap the destination map to populate with extracted field name-value pairs
     * @throws ValueException if extraction fails and {@link #skipOnFailure()} returns {@code false}
     */
    void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
            throws ValueException;

    /**
     * Returns whether extraction failures should be silently ignored.
     *
     * <p>When {@code true}, individual field extraction failures do not cause the entire operation
     * to fail. Problematic fields are simply omitted from the result, and extraction continues for
     * remaining fields.
     *
     * <p>When {@code false}, any extraction failure immediately throws a {@link ValueException},
     * aborting the entire extraction operation.
     *
     * @return {@code true} if extraction failures should be skipped, {@code false} if they should
     *     cause the operation to fail
     */
    boolean skipOnFailure();

    /**
     * Returns whether non-scalar values should be serialized and included in the result.
     *
     * <p>When {@code true}, non-scalar values (objects, arrays) are serialized to JSON strings and
     * included in the extracted field map.
     *
     * <p>When {@code false}, non-scalar values are omitted from the result. If {@link
     * #skipOnFailure()} is also {@code false}, encountering a non-scalar value throws a {@link
     * ValueException}.
     *
     * @return {@code true} if non-scalar values should be serialized to strings, {@code false} if
     *     they should be omitted
     */
    boolean mapNonScalars();

    /**
     * Returns the set of field names that this extractor is configured to produce.
     *
     * <p>For <strong>named field extractors</strong>, this returns the predefined field names
     * configured at construction time. These names are known statically and represent the complete
     * set of fields that may appear in extraction results.
     *
     * <p>For <strong>discovered field extractors</strong>, field names cannot be known until
     * extraction time, so this method returns an empty set.
     *
     * <p>For <strong>composite extractors</strong>, this returns the union of all mapped fields
     * from all composed extractors.
     *
     * @return an immutable set of field names that this extractor may produce, or an empty set if
     *     field names are determined dynamically at runtime
     */
    Set<String> mappedFields();
}
