
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

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;

import java.util.Collections;
import java.util.Map;

public interface DataExtractor<K, V> {

    SchemaAndValues extractData(KafkaRecord<K, V> record);

    Schema schema();

    default boolean skipOnFailure() {
        return false;
    }

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
