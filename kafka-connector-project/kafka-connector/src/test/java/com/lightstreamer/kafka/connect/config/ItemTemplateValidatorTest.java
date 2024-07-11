
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
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.ITEM_TEMPLATES;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ItemTemplateValidatorTest {

    ItemTemplateValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new ItemTemplateValidator();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(
            strings = {
                "template-name:template-value",
                "template-name1:template-value1;template-name2:template-value2"
            })
    public void shouldValidate(Object value) {
        assertDoesNotThrow(() -> validator.ensureValid(ITEM_TEMPLATES, value));
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    VALUE                         | EXPECTED_ERR_MESSAGE
                    ''                            | Invalid value for configuration "item.templates": Must be a non-empty semicolon-separated list
                    '  '                          | Invalid value for configuration "item.templates": Must be a non-empty semicolon-separated list
                    ;                             | Invalid value for configuration "item.templates": Must be a semicolon-separated list of non-empty strings
                    :                             | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-value>
                    template-name:template-value; | Invalid value for configuration "item.templates": Must be a semicolon-separated list of non-empty strings
                    template-name                 | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-value>
                    template-name:                | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-value>
                    :template-value               | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-value>
                """)
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> validator.ensureValid(ITEM_TEMPLATES, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldValidateDueToNonStringValue() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> validator.ensureValid(ITEM_TEMPLATES, new Object()));
        assertThat(ce.getMessage())
                .isEqualTo("Invalid value for configuration \"item.templates\": Must be a string");
    }
}
