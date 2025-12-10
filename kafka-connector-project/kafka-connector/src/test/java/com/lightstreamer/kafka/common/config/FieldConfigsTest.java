
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class FieldConfigsTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "#{VALUE}",
                "#{OFFSET}",
                "#{PARTITION}",
                "#{OFFSET}",
                "#{TOPIC}",
                "#{HEADERS}"
            })
    void shouldCreateAndMakeExtractor(String expression) throws ExtractionException {
        Map<String, String> fieldMappings = Map.of("field1", expression);
        FieldConfigs configs = FieldConfigs.from(fieldMappings);

        boolean[] skipOnFailures = {false, true};
        boolean[] mapScalars = {false, true};
        for (boolean skip : skipOnFailures) {
            for (boolean mapNonScalars : mapScalars) {
                FieldsExtractor<String, String> extractor =
                        configs.fieldsExtractor(
                                OthersSelectorSuppliers.String(), skip, mapNonScalars);
                assertThat(extractor.mappedFields()).isEqualTo(fieldMappings.keySet());
                assertThat(extractor.skipOnFailure()).isEqualTo(skip);
                assertThat(extractor.mapNonScalars()).isEqualTo(mapNonScalars);
            }
        }
    }

    @Test
    void shouldFailCreateExtractor() {
        Map<String, String> fieldMappings = Map.of("field1", "#{VALUE.notAllowedAttrib}");
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () ->
                                configs.fieldsExtractor(
                                        OthersSelectorSuppliers.String(), false, false));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found the invalid expression [VALUE.notAllowedAttrib] for scalar values");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "#{VALUE}",
                "#{VALUE.attrib}",
                "#{OFFSET}",
                "#{PARTITION}",
                "#{OFFSET}",
                "#{TOPIC}",
                "#{HEADERS}"
            })
    void shouldCreateAndMakeExtractorForComplexSupplier(String expression)
            throws ExtractionException {
        Map<String, String> fieldMappings = Map.of("field1", expression);
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        FieldsExtractor<Object, Object> extractor =
                configs.fieldsExtractor(TestSelectorSuppliers.Object(), false, false);
        assertThat(extractor.mappedFields()).isEqualTo(fieldMappings.keySet());
    }

    @ParameterizedTest
    @EmptySource
    @NullAndEmptySource
    @ValueSource(strings = {"#{}", ".", "\\", " "})
    void shouldFailCreationDueToInvalidWrapperExpression(String expression) {
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("field1", expression);
        ConfigException ee =
                assertThrows(ConfigException.class, () -> FieldConfigs.from(fieldMappings));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Got the following error while evaluating the field [field1] containing the expression ["
                                + expression
                                + "]: <Invalid expression>");
    }

    @ParameterizedTest
    @ValueSource(strings = {"#{.}", "#{...}"})
    void shouldFailCreationDueToMissingRootTokens(String fieldExpression) {
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("field1", fieldExpression);
        ConfigException ee =
                assertThrows(ConfigException.class, () -> FieldConfigs.from(fieldMappings));

        assertThat(ee.getMessage())
                .isEqualTo(
                        "Got the following error while evaluating the field [field1] containing the expression ["
                                + fieldExpression
                                + "]: <Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]>");
    }
}
