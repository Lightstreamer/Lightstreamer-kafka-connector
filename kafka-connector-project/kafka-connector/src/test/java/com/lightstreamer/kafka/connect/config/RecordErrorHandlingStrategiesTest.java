
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
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;

import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;

public class RecordErrorHandlingStrategiesTest {

    Validator validator = RecordErrorHandlingStrategies.VALIDATOR;

    @ParameterizedTest
    @EnumSource
    public void shouldValidate(RecordErrorHandlingStrategy value) {
        assertDoesNotThrow(
                () -> validator.ensureValid(RECORD_EXTRACTION_ERROR_STRATEGY, value.toString()));
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    VALUE     | EXPECTED_ERR_MESSAGE
                              | Invalid value for configuration "record.extraction.error.strategy": Must be non-null
                    ''        | Invalid value for configuration "record.extraction.error.strategy": Must be a non-empty string
                    '  '      | Invalid value for configuration "record.extraction.error.strategy": Must be a non-empty string
                    NON_VALID | Invalid value for configuration "record.extraction.error.strategy": Must be a valid strategy
                """)
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> validator.ensureValid(RECORD_EXTRACTION_ERROR_STRATEGY, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldValidateDueToNonStringValue() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                validator.ensureValid(
                                        RECORD_EXTRACTION_ERROR_STRATEGY, new Object()));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"record.extraction.error.strategy\": Must be a string");
    }

    @Test
    public void shouldRecommend() {
        Recommender recommender = RecordErrorHandlingStrategies.RECOMMENDER;
        assertThat(recommender.visible(RECORD_EXTRACTION_ERROR_STRATEGY, Collections.emptyMap()));
        List<Object> validValues =
                recommender.validValues(RECORD_EXTRACTION_ERROR_STRATEGY, Collections.emptyMap());
        assertThat(validValues)
                .containsExactly(
                        RecordErrorHandlingStrategy.FORWARD_TO_DLQ.toString(),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE.toString(),
                        RecordErrorHandlingStrategy.TERMINATE_TASK.toString());
    }
}
