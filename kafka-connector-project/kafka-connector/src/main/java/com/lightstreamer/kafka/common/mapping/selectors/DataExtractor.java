
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

public interface DataExtractor<K, V> {

    /**
     * Extracts schema and values from the given Kafka record.
     *
     * @param record the Kafka record containing the key and value to be processed
     * @return a {@link SchemaAndValues} object representing the extracted schema and values
     */
    SchemaAndValues extractData(KafkaRecord<K, V> record) throws ValueException;

    /**
     * Retrieves the schema associated with the data extractor.
     *
     * @return the {@link Schema} object representing the structure of the data.
     */
    Schema schema();

    /**
     * Determines whether the operation should be skipped in case of a failure. By default, this
     * method returns {@code false}, indicating that the operation should not be skipped on failure.
     *
     * @return {@code true} if the operation should be skipped on failure, {@code false} otherwise
     */
    default boolean skipOnFailure() {
        return false;
    }

    /**
     * Determines whether non-scalar values should be mapped. By default, this method returns {@code
     * false}, indicating that non-scalar values are not mapped.
     *
     * @return {@code true} if non-scalar values should be mapped, {@code false} otherwise
     */
    default boolean mapNonScalars() {
        return false;
    }

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

    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, TemplateExpression expression)
            throws ExtractionException {
        return extractor(sSuppliers, expression.prefix(), expression.params(), false, false);
    }

    public static <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> sSuppliers, String schemaName)
            throws ExtractionException {
        return extractor(sSuppliers, schemaName, Collections.emptyMap(), false, false);
    }
}
