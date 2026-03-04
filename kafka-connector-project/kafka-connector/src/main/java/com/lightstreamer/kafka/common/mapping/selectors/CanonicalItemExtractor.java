
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

/**
 * Interface for extracting canonical Lightstreamer item names from Kafka records through template
 * expansion and data transformation.
 *
 * <p>This interface serves as a core component in the Kafka-to-Lightstreamer mapping process,
 * enabling the generation of deterministic item identifiers used by {@link
 * com.lightstreamer.kafka.common.mapping.RecordMapper} for subscriber routing and item-based data
 * distribution.
 *
 * <h2>Canonical Item Names</h2>
 *
 * <p>A canonical item name is a string identifier produced by evaluating template expressions
 * against a Kafka record's content. These identifiers follow a specific format and serve multiple
 * purposes:
 *
 * <ul>
 *   <li>Uniquely identify data streams for Lightstreamer subscribers
 *   <li>Enable routing of record updates to the appropriate subscribers
 *   <li>Support parameterized subscriptions through template parameter extraction
 *   <li>Provide deterministic, consistent identifiers for identical record content
 * </ul>
 *
 * <h2>Item Name Format</h2>
 *
 * <p>Canonical item names follow the format: {@code "prefix-[param1=value1,param2=value2]"}
 *
 * <ul>
 *   <li><strong>Prefix:</strong> The schema name defining the item structure
 *   <li><strong>Parameters:</strong> Key-value pairs extracted from the record, enclosed in
 *       brackets and comma-separated
 *   <li><strong>Order:</strong> Parameters are sorted <em>alphanumerically by name</em>, regardless
 *       of template expression order. This is critical for item name equality and subscriber
 *       matching.
 * </ul>
 *
 * <p><b>Example:</b> {@code "stock-[exchange=NASDAQ,symbol=AAPL]"} where "exchange" precedes
 * "symbol" alphabetically.
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Template: "stock-#{symbol=VALUE.symbol,exchange=VALUE.exchange}"
 * CanonicalItemExtractor<String, JsonNode> extractor = // ... created via DataExtractors
 *
 * // For a record with value: {"symbol": "AAPL", "exchange": "NASDAQ", "price": 150.25}
 * String itemName = extractor.extractCanonicalItem(record);
 * // Result: "stock-[exchange=NASDAQ,symbol=AAPL]" (alphabetically sorted)
 *
 * Schema schema = extractor.schema();
 * // schema.name() returns "stock"
 * }</pre>
 *
 * <h2>Template Expressions</h2>
 *
 * <p>Extractors are configured with template expressions that define:
 *
 * <ul>
 *   <li><strong>Item name prefix:</strong> The base name for the item type
 *   <li><strong>Parameters:</strong> Which data to extract from the record for item identification
 *   <li><strong>Parameter order:</strong> Automatically sorted alphanumerically by name in the
 *       resulting item name
 *   <li><strong>Data sources:</strong> Where to extract parameter values (KEY, VALUE, HEADERS,
 *       metadata)
 * </ul>
 *
 * <h2>Implementation Notes</h2>
 *
 * <p>Implementations of this interface:
 *
 * <ul>
 *   <li>Must produce deterministic results for identical input records
 *   <li>Must sort parameters alphanumerically by name in the resulting item names
 *   <li>Should be thread-safe for concurrent extraction operations
 *   <li>Must handle missing or null parameter values according to configuration
 *   <li>Should validate extracted values during template expansion
 * </ul>
 *
 * <p>The primary implementation is provided by {@link DataExtractors#canonicalItemExtractor}.
 *
 * @param <K> the type of the Kafka record key
 * @param <V> the type of the Kafka record value
 * @see DataExtractors#canonicalItemExtractor
 * @see com.lightstreamer.kafka.common.mapping.RecordMapper
 * @see Schema
 */
public interface CanonicalItemExtractor<K, V> {

    /**
     * Extracts and returns the canonical item name from the given Kafka record through template
     * expansion.
     *
     * <p>This method evaluates configured template expressions against the provided record to
     * generate a canonical item identifier. The extraction process:
     *
     * <ul>
     *   <li>Evaluates template parameters against the record's key, value, headers, and metadata
     *   <li>Performs data extraction and type conversion as needed
     *   <li>Produces canonical item names in the format: {@code "prefix-[param1=value1,...]"}
     *   <li>Sorts parameters alphanumerically by parameter name
     *   <li>Returns deterministic, consistent identifiers for identical record content
     * </ul>
     *
     * <p><strong>Example:</strong> For a record with value {@code {"symbol": "AAPL", "price":
     * 150.25}} and template {@code "stock-#{symbol=VALUE.symbol}"}, this method returns {@code
     * "stock-[symbol=AAPL]"}.
     *
     * @param record the Kafka record containing the key, value, headers, and metadata to extract
     *     the canonical item identifier from
     * @return the canonical item name as a String representation following the format
     *     "prefix-[param=value,...]"; never {@code null} but may be just the prefix for templates
     *     with no parameters
     * @throws ValueException if template expansion fails due to data extraction issues, type
     *     conversion errors, or if the record contains data that cannot be properly processed by
     *     the configured template expressions
     */
    String extractCanonicalItem(KafkaRecord<K, V> record) throws ValueException;

    /**
     * Retrieves the schema associated with this canonical item extractor, defining the structure
     * for items produced by this extractor.
     *
     * <p>The schema serves multiple purposes:
     *
     * <ul>
     *   <li>Provides the item name prefix used in canonical item generation
     *   <li>Defines the structure of data available for updates to subscribers
     *   <li>Establishes the contract between the extractor and update data structure
     *   <li>Enables validation and configuration of the data adapter
     * </ul>
     *
     * <p>The schema's name component is used as the prefix in all canonical item names generated by
     * this extractor.
     *
     * @return the {@link Schema} object representing the structure definition for items produced by
     *     this extractor; never {@code null}
     */
    Schema schema();
}
