
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

import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import java.util.HashMap;
import java.util.Map;

public class FieldConfigs {

    private static String SCHEMA_NAME = "fields";

    public static FieldConfigs from(Map<String, String> configs) {
        return new FieldConfigs(configs);
    }

    private final Map<String, ExtractionExpression> expressions = new HashMap<>();

    private FieldConfigs(Map<String, String> fieldsMapping) {
        for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
            String fieldName = entry.getKey();
            String wrappedExpression = entry.getValue();
            try {
                expressions.put(fieldName, Expressions.Wrapped(wrappedExpression));
            } catch (ExpressionException e) {
                throw new ConfigException(
                        "Found the invalid expression [%s] while evaluating [%s]: <%s>"
                                .formatted(wrappedExpression, fieldName, e.getMessage()));
            }
        }
    }

    public Map<String, ExtractionExpression> expressions() {
        return new HashMap<>(expressions);
    }

    public boolean contains(String fieldName) {
        return expressions.containsKey(fieldName);
    }

    public ExtractionExpression getExression(String fieldName) {
        return expressions.get(fieldName);
    }

    public <K, V> DataExtractor<K, V> extractor(KeyValueSelectorSuppliers<K, V> selectorSuppliers)
            throws ExtractionException {

        return DataExtractor.<K, V>builder()
                .withSuppliers(selectorSuppliers)
                .withSchemaName(SCHEMA_NAME)
                .withExpressions(expressions)
                .build();
    }
}
