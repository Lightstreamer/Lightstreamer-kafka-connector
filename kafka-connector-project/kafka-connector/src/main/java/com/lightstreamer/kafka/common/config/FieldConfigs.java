
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

package com.lightstreamer.kafka.common.config;

import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractors;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldConfigs {

    private static String SCHEMA_NAME = "fields";

    public static FieldConfigs from(Map<String, String> configs) {
        return new FieldConfigs(configs);
    }

    private final Map<String, ExtractionExpression> boundFieldExpressions = new HashMap<>();

    private final Map<String, ExtractionExpression> autoBoundFieldExpressions = new HashMap<>();

    private FieldConfigs(Map<String, String> fieldsMapping) {
        for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
            String fieldName = entry.getKey();
            String wrappedExpression = entry.getValue();
            try {
                if ("*".equals(fieldName)) {
                    autoBoundFieldExpressions.put(
                            fieldName, Expressions.Wrapped(wrappedExpression));
                    continue;
                }
                boundFieldExpressions.put(fieldName, Expressions.Wrapped(wrappedExpression));
            } catch (ExpressionException e) {
                throw new ConfigException(
                        "Got the following error while evaluating the field [%s] containing the expression [%s]: <%s>"
                                .formatted(fieldName, wrappedExpression, e.getMessage()));
            }
        }
    }

    public Map<String, ExtractionExpression> expressions() {
        return new HashMap<>(boundFieldExpressions);
    }

    public ExtractionExpression getExpression(String fieldName) {
        return boundFieldExpressions.get(fieldName);
    }

    public <K, V> DataExtractor<K, V> extractor(
            KeyValueSelectorSuppliers<K, V> selectorSuppliers,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {
        return DataExtractor.extractor(
                selectorSuppliers,
                SCHEMA_NAME,
                boundFieldExpressions,
                skipOnFailure,
                mapNonScalars);
    }

    public <K, V> FieldsExtractor<K, V> fieldsExtractor(
            KeyValueSelectorSuppliers<K, V> selectorSuppliers,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {

        List<FieldsExtractor<K, V>> fieldExtractors = new java.util.ArrayList<>();
        if (!autoBoundFieldExpressions.isEmpty()) {
            fieldExtractors.add(
                    DataExtractors.dynamicFieldsExtractor(
                            selectorSuppliers, autoBoundFieldExpressions, skipOnFailure));
        }
        if (!boundFieldExpressions.isEmpty()) {
            fieldExtractors.add(
                    DataExtractors.staticFieldsExtractor(
                            selectorSuppliers,
                            boundFieldExpressions,
                            skipOnFailure,
                            mapNonScalars));
        }

        if (fieldExtractors.size() == 1) {
            return fieldExtractors.get(0);
        }

        return DataExtractors.composedFieldsExtractor(fieldExtractors);
    }
}
