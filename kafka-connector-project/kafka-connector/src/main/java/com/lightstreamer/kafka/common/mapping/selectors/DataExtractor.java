
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

import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import java.util.Collections;
import java.util.Map;

public interface DataExtractor<K, V> {

    SchemaAndValues extractData(KafkaRecord<K, V> record);

    // SchemaAndValues extractData2_0(KafkaRecord<K, V> record);

    // SchemaAndValues extractDataOld1_0(KafkaRecord<K, V> record);

    // SchemaAndValues extractDataOld1_1(KafkaRecord<K, V> record);

    Schema schema();

    public static <K, V> Builder<K, V> builder() {
        return new DataExtractorSupport.DataExtractorBuilder<>();
    }

    public interface Builder<K, V> {

        Builder<K, V> withSuppliers(KeyValueSelectorSuppliers<K, V> sSuppliers);

        Builder<K, V> withExpressions(Map<String, ExtractionExpression> expressions);

        Builder<K, V> withSchemaName(String schema);

        DataExtractor<K, V> build() throws ExtractionException;
    }

    public static <K, V> DataExtractor<K, V> withSimple(
            KeyValueSelectorSuppliers<K, V> sSuppliers, String schema) throws ExtractionException {
        return with(sSuppliers, schema, Collections.emptyMap());
    }

    public static <K, V> DataExtractor<K, V> withBoundExpression(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            String schema,
            String parameter,
            String expression)
            throws ExtractionException {
        return with(sSuppliers, schema, Map.of(parameter, Expression(expression)));
    }

    public static <K, V> DataExtractor<K, V> with(
            KeyValueSelectorSuppliers<K, V> sSuppliers,
            String schema,
            Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        return DataExtractor.<K, V>builder()
                .withSuppliers(sSuppliers)
                .withSchemaName(schema)
                .withExpressions(expressions)
                .build();
    }
}
