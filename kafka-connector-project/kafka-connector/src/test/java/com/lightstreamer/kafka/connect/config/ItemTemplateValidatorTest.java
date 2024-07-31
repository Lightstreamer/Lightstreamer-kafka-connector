
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
                "template-name:template-prefix-#{param=VALUE}",
                "  template-name  :  template-prefix-#{param=VALUE}  ",
                "template-name1:template1-prefix1-#{param=VALUE};template-name2:template-prefix2-#{param=VALUE}",
                "template-name1:template1-prefix1-#{param=VALUE};   template-name2:template-prefix2-#{param=VALUE}  "
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
                    VALUE                                            | EXPECTED_ERR_MESSAGE
                    ''                                               | Invalid value for configuration "item.templates": Must be a non-empty semicolon-separated list
                    '  '                                             | Invalid value for configuration "item.templates": Must be a non-empty semicolon-separated list
                    ;                                                | Invalid value for configuration "item.templates": Must be a semicolon-separated list of non-empty strings
                    :                                                | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-expression>
                    template-name:t1-prefix-#{p1=VALUE};             | Invalid value for configuration "item.templates": Must be a semicolon-separated list of non-empty strings
                    template-name:t1-prefix-#{}                      | Invalid value for configuration "item.templates": Template expression must be expressed in the form <template-prefix-#{par1=val1,...parN=valN}
                    template-name                                    | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-expression>
                    template-name:                                   | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-expression>
                    :t1-prefix-#{p1=v1}                              | Invalid value for configuration "item.templates": Each entry must be expressed in the form <template-name:template-expression>
                    t1:t1-prefix-#{p1=KEY};t1:t1-prefix2-#{p2=VALUE} | Invalid value for configuration "item.templates": Duplicate key "t1"
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
