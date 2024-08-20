
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

package com.lightstreamer.kafka.connect.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_MAPPING;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class RecordMappingsValidatorTest {

    RecordMappingValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new RecordMappingValidator();
    }

    static Stream<Arguments> wrongValues() {
        return Stream.of(
                // Null record.mapping
                arguments(
                        null,
                        "Invalid value for configuration \"record.mapping\": Must be non-null"),
                // Non List
                arguments(
                        new Object(),
                        "Invalid value for configuration \"record.mapping\": Must be a list"),
                arguments(
                        List.of(1, 2, 3),
                        "Invalid value for configuration \"record.mapping\": Must be a list of non-empty strings"),

                // Empty record.mapping
                arguments(
                        Collections.emptyList(),
                        "Invalid value for configuration \"record.mapping\": Must be a non-empty list"),
                // List of empty record.mapping
                arguments(
                        List.of(""),
                        "Invalid value for configuration \"record.mapping\": Must be a list of non-empty strings"),
                arguments(
                        List.of("", ""),
                        "Invalid value for configuration \"record.mapping\": Must be a list of non-empty strings"),
                arguments(
                        List.of(" ", ""),
                        "Invalid value for configuration \"record.mapping\": Must be a list of non-empty strings"),
                // List of mixed non-empty/empty-strings
                arguments(
                        List.of("field1"),
                        "Invalid value for configuration \"record.mapping\": Each entry must be in the form [fieldName]:[extractionExpression]"),
                // Invalid expression
                arguments(
                        List.of("field:VALUE"),
                        "Invalid value for configuration \"record.mapping\": Extraction expression must be in the form #{...}"),
                // List of duplicate entry
                arguments(
                        List.of("field1:#{VALUE}", "field1:#{KEY}"),
                        "Invalid value for configuration \"record.mapping\": Duplicate key \"field1\""));
    }

    @ParameterizedTest
    @MethodSource("wrongValues")
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> validator.ensureValid(RECORD_MAPPING, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> values() {
        return Stream.of(
                arguments(List.of("field1:#{VALUE}")),
                arguments(List.of("  field1 :  #{KEY}  ")),
                arguments(List.of("field1:#{PARTITION}", "field2:#{VALUE.attrib1.attrib2}")));
    }

    @ParameterizedTest
    @MethodSource("values")
    public void shouldValidate(Object value) {
        assertDoesNotThrow(() -> validator.ensureValid(RECORD_MAPPING, value));
    }
}
