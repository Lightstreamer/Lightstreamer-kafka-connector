
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
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;
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
    @ValueSource(strings = {"#{VALUE}", "#{OFFSET}", "#{PARTITION}", "#{OFFSET}", "#{TOPIC}"})
    void shouldCreateAndMakeExtractor(String expression) throws ExtractionException {
        Map<String, String> fieldMappings = Map.of("field1", expression);
        FieldConfigs configs = FieldConfigs.from(fieldMappings);

        boolean[] skipOnFailures = {false, true};
        boolean[] mapScalars = {false, true};
        for (boolean skip : skipOnFailures) {
            for (boolean mapNonScalars : mapScalars) {
                DataExtractor<String, String> extractor =
                        configs.extractor(OthersSelectorSuppliers.String(), skip, mapNonScalars);
                Schema schema = extractor.schema();
                assertThat(schema.name()).isEqualTo("fields");
                assertThat(schema.keys()).isEqualTo(fieldMappings.keySet());
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
                        () -> configs.extractor(OthersSelectorSuppliers.String(), false, false));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found the invalid expression [VALUE.notAllowedAttrib] for scalar values while evaluating [field1]");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "#{VALUE}",
                "#{VALUE.attrib}",
                "#{OFFSET}",
                "#{PARTITION}",
                "#{OFFSET}",
                "#{TOPIC}"
            })
    void shouldCreateAndMakeExtractorForComplexSupplier(String expression)
            throws ExtractionException {
        Map<String, String> fieldMappings = Map.of("field1", expression);
        FieldConfigs configs = FieldConfigs.from(fieldMappings);
        DataExtractor<Object, Object> extractor =
                configs.extractor(TestSelectorSuppliers.Object(), false, false);
        Schema schema = extractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).isEqualTo(fieldMappings.keySet());
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
                        "Found the invalid expression ["
                                + expression
                                + "] while evaluating [field1]: <Invalid expression>");
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
                        "Found the invalid expression ["
                                + fieldExpression
                                + "] while evaluating [field1]: <Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]>");
    }
}
