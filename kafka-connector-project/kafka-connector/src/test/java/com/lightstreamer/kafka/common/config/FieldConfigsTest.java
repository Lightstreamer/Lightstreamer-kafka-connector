
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

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers.String;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonKeyJsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class FieldConfigsTest {

    static Stream<Arguments> namedFieldMapping() {
        return Stream.of(
                Arguments.of(
                        Map.of("field1", "#{VALUE}", "field2", "#{KEY}"), String(), false, true),
                Arguments.of(
                        Map.of("field1", "#{VALUE.attrib}", "field2", "#{HEADERS}"),
                        JsonValue(),
                        true,
                        false),
                Arguments.of(
                        Map.of("field1", "#{VALUE.attrib}", "field2", "#{KEY.attrib}"),
                        JsonKeyJsonValue(),
                        true,
                        true));
    }

    @ParameterizedTest
    @MethodSource("namedFieldMapping")
    public void shouldCreateAndMakeNamedFieldsExtractor(
            Map<String, String> fieldMappings,
            KeyValueSelectorSuppliers<?, ?> suppliers,
            boolean mapNonScalars,
            boolean skipOnFailure)
            throws ExtractionException {
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        checkBoundExpressions(fieldMappings, configs.namedFieldsExpressions());
        assertThat(configs.discoveredFieldsExpressions()).isEmpty();

        boolean[] skipOnFailures = {false, true};
        for (boolean skip : skipOnFailures) {
            FieldsExtractor<?, ?> extractor =
                    configs.fieldsExtractor(suppliers, skip, mapNonScalars);
            assertThat(extractor.mappedFields()).isEqualTo(fieldMappings.keySet());
            assertThat(extractor.skipOnFailure()).isEqualTo(skip);
            assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);
        }
    }

    private void checkBoundExpressions(
            Map<String, String> fieldMappings, Map<String, ExtractionExpression> boundExpressions) {
        assertThat(boundExpressions.keySet()).isEqualTo(fieldMappings.keySet());
        for (Map.Entry<String, ExtractionExpression> entry : boundExpressions.entrySet()) {
            String fieldName = entry.getKey();
            ExtractionExpression expr = entry.getValue();
            String wrapped = fieldMappings.get(fieldName);
            String unwrapped = wrapped.substring(2, wrapped.length() - 1);
            assertThat(expr.toString()).isEqualTo(unwrapped);
        }
    }

    static Stream<Arguments> dynamicFieldsMapping() {
        return Stream.of(
                Arguments.of(Map.of("*", "#{VALUE.list.*}"), JsonKeyJsonValue(), false),
                Arguments.of(Map.of("*", "#{VALUE.map.*}"), JsonKeyJsonValue(), true));
    }

    @ParameterizedTest
    @MethodSource("dynamicFieldsMapping")
    public void shouldCreateAndMakeStaticDynamicExtractor(
            Map<String, String> fieldMappings,
            KeyValueSelectorSuppliers<?, ?> suppliers,
            boolean mapNonScalars)
            throws ExtractionException {
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        assertThat(configs.namedFieldsExpressions()).isEmpty();
        assertThat(configs.discoveredFieldsExpressions()).hasSize(1);

        boolean[] skipOnFailures = {false, true};
        for (boolean skip : skipOnFailures) {
            // The mapNonScalars parameter is ignored for dynamic extractors, as they always map
            // non-scalar values
            FieldsExtractor<?, ?> extractor =
                    configs.fieldsExtractor(suppliers, skip, mapNonScalars);
            assertThat(extractor.mapNonScalars()).isEqualTo(true);
            assertThat(extractor.mappedFields()).isEmpty();
            assertThat(extractor.skipOnFailure()).isEqualTo(skip);
        }
    }

    static Stream<Arguments> mixedDynamicFieldsMapping() {
        return Stream.of(
                Arguments.of(
                        Map.of("field1", "#{VALUE}", "field2", "#{KEY}"),
                        String(),
                        false, // mapNonScalars
                        true, // skipOnFailure
                        false, // expectedComposed
                        Set.of("field1", "field2")),
                Arguments.of(
                        Map.of("*", "#{VALUE.list.*}"),
                        JsonKeyJsonValue(),
                        true, // mapNonScalars
                        true, // skipOnFailure
                        false, // expectedComposed
                        Collections.emptySet()),
                Arguments.of(
                        Map.of("*", "#{VALUE.list.*}", "field1", "#{KEY}"),
                        JsonKeyJsonValue(),
                        true, // mapNonScalars
                        false, // skipOnFailure
                        true, // expectedComposed
                        Set.of("field1")));
    }

    @ParameterizedTest
    @MethodSource("mixedDynamicFieldsMapping")
    public void shouldCreateComposedExtractor(
            Map<String, String> fieldMappings,
            KeyValueSelectorSuppliers<?, ?> suppliers,
            boolean mapNonScalars,
            boolean skipOnFailure,
            boolean expectedComposed,
            Set<String> expectedMappedFields)
            throws ExtractionException {

        // Extract static field mappings for later verification
        Map<String, String> staticFieldMappings = new HashMap<>();
        Map<String, String> dynamicFieldMappings = new HashMap<>();
        for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
            if (entry.getKey().equals("*")) {
                dynamicFieldMappings.put(entry.getKey(), entry.getValue());
            } else {
                staticFieldMappings.put(entry.getKey(), entry.getValue());
            }
        }

        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        checkBoundExpressions(staticFieldMappings, configs.namedFieldsExpressions());
        assertThat(configs.discoveredFieldsExpressions()).hasSize(dynamicFieldMappings.size());

        FieldsExtractor<?, ?> extractor =
                configs.fieldsExtractor(JsonKeyJsonValue(), skipOnFailure, mapNonScalars);
        assertThat(extractor.mappedFields()).isEqualTo(expectedMappedFields);
        if (!expectedComposed) {
            assertThat(extractor.skipOnFailure()).isEqualTo(skipOnFailure);
            assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);
        }
    }

    @Test
    public void shouldFailCreateExtractor() {
        Map<String, String> fieldMappings = Map.of("field1", "#{VALUE.notAllowedAttrib}");
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> configs.fieldsExtractor(String(), false, false));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Found the invalid expression [VALUE.notAllowedAttrib] for scalar values");
    }

    @ParameterizedTest
    @EmptySource
    @NullAndEmptySource
    @ValueSource(strings = {"#{}", ".", "\\", " "})
    public void shouldFailCreationDueToInvalidWrappedExpression(String expression) {
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("field1", expression);
        ConfigException ee =
                assertThrows(ConfigException.class, () -> FieldConfigs.from(fieldMappings));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Got the following error while evaluating the field [field1] containing the expression ["
                                + expression
                                + "]: <Invalid expression>");
    }

    @ParameterizedTest
    @ValueSource(strings = {".", "..."})
    public void shouldFailCreationDueToMissingRootTokens(String fieldExpression) {
        String wrappedExpression = "#{" + fieldExpression + "}";
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("field1", wrappedExpression);
        ConfigException ee =
                assertThrows(ConfigException.class, () -> FieldConfigs.from(fieldMappings));

        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Got the following error while evaluating the field [field1] containing the expression ["
                                + wrappedExpression
                                + "]: <Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS] in the expression ["
                                + fieldExpression
                                + "]>");
    }
}
