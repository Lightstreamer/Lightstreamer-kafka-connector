
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedWithWildcards;

import com.lightstreamer.kafka.common.mapping.selectors.DataExtractors;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldConfigs {

    public static FieldConfigs from(Map<String, String> configs) throws ConfigException {
        return new FieldConfigs(configs);
    }

    private final Map<String, ExtractionExpression> namedFieldExpressions = new HashMap<>();
    private final Map<String, ExtractionExpression> discoveredFieldsExpressions = new HashMap<>();

    private FieldConfigs(Map<String, String> fieldsMapping) throws ConfigException {
        for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
            String fieldName = entry.getKey();
            String expression = entry.getValue();
            try {
                if ("*".equals(fieldName)) {
                    discoveredFieldsExpressions.put(fieldName, WrappedWithWildcards(expression));
                    continue;
                }
                namedFieldExpressions.put(fieldName, Wrapped(expression));
            } catch (ExpressionException e) {
                throw new ConfigException(
                        "Got the following error while evaluating the field [%s] containing the expression [%s]: <%s>"
                                .formatted(fieldName, expression, e.getMessage()));
            }
        }
    }

    public Map<String, ExtractionExpression> namedFieldsExpressions() {
        return new HashMap<>(namedFieldExpressions);
    }

    public Map<String, ExtractionExpression> discoveredFieldsExpressions() {
        return new HashMap<>(discoveredFieldsExpressions);
    }

    public <K, V> FieldsExtractor<K, V> fieldsExtractor(
            KeyValueSelectorSuppliers<K, V> selectorSuppliers,
            boolean skipOnFailure,
            boolean mapNonScalars)
            throws ExtractionException {

        List<FieldsExtractor<K, V>> fieldExtractors = new java.util.ArrayList<>();
        if (!discoveredFieldsExpressions.isEmpty()) {
            fieldExtractors.add(
                    DataExtractors.discoveredFieldsExtractor(
                            selectorSuppliers,
                            discoveredFieldsExpressions.values(),
                            skipOnFailure));
        }
        if (!namedFieldExpressions.isEmpty()) {
            fieldExtractors.add(
                    DataExtractors.namedFieldsExtractor(
                            selectorSuppliers,
                            namedFieldExpressions,
                            skipOnFailure,
                            mapNonScalars));
        }

        if (fieldExtractors.size() == 1) {
            return fieldExtractors.get(0);
        }

        return DataExtractors.composedFieldsExtractor(fieldExtractors);
    }
}
