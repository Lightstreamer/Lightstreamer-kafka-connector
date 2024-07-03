
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

package com.lightstreamer.kafka.mapping;

import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.Parsers;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public interface Fields {

    static String SCHEMA_NAME = "fields";

    static Pattern FIELD_MAPPING = Pattern.compile(Parsers.SELECTION_REGEX);

    // Strips the enclosing notation "#{...}" from the the specifed Entry value
    private static String stripExpression(Map.Entry<String, String> entry) {
        String param = entry.getKey();
        String originalExpression = entry.getValue();
        Matcher matcher = FIELD_MAPPING.matcher(originalExpression);
        if (!matcher.matches()) {
            ExpressionException.throwInvalidFieldExpression(param, originalExpression);
        }

        return matcher.group(1);
    }

    public static <K, V> ValuesExtractor<K, V> fromMapping(
            Map<String, String> fieldsMapping, SelectorSuppliers<K, V> selectorSuppliers) {

        Map<String, String> strippedExpressions =
                fieldsMapping.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, Fields::stripExpression));

        return ValuesExtractor.<K, V>builder()
                .withSuppliers(selectorSuppliers)
                .withSchemaName(SCHEMA_NAME)
                .withExpressions(strippedExpressions)
                .build();
    }
}
