
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
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.TOPIC_MAPPINGS;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TopicMappingValidatorTest {

    TopicMappingsValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new TopicMappingsValidator();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "topic-name:item",
                "  topic-name  :  item  ",
                "topic-name1:item1;topic-name2:item2",
                "topic-name1:item1 ;   topic-name2:item2",
            })
    public void shouldValidate(Object value) {
        assertDoesNotThrow(() -> validator.ensureValid(TOPIC_MAPPINGS, value));
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '@',
            textBlock =
                    """
                    VALUE                               @ EXPECTED_ERR_MESSAGE
                                                        @ Invalid value for configuration "topic.mappings": Must be non-null
                    ''                                  @ Invalid value for configuration "topic.mappings": Must be a non-empty semicolon-separated list
                    '  '                                @ Invalid value for configuration "topic.mappings": Must be a non-empty semicolon-separated list
                    ;                                   @ Invalid value for configuration "topic.mappings": Must be a semicolon-separated list of non-empty strings
                    topic-name:item;                    @ Invalid value for configuration "topic.mappings": Must be a semicolon-separated list of non-empty strings
                    :                                   @ Invalid value for configuration "topic.mappings": Each entry must be in the form [topicName]:[mappingList]
                    topic-name                          @ Invalid value for configuration "topic.mappings": Each entry must be in the form [topicName]:[mappingList]
                    topic-name:                         @ Invalid value for configuration "topic.mappings": Each entry must be in the form [topicName]:[mappingList]
                    topic-name:item1;topic-name:item2   @ Invalid value for configuration "topic.mappings": Duplicate key "topic-name"
                    topic-name:item1,                   @ Invalid value for configuration "topic.mappings": Mapping list must be in the form [item-template.template1|item1],...,[item-template.templateN|itemN]
                    topic-name:,                        @ Invalid value for configuration "topic.mappings": Mapping list must be in the form [item-template.template1|item1],...,[item-template.templateN|itemN]
                    topic-name:item1,;topic-name2:item2 @ Invalid value for configuration "topic.mappings": Mapping list must be in the form [item-template.template1|item1],...,[item-template.templateN|itemN]
                """)
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> validator.ensureValid(TOPIC_MAPPINGS, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldNotValidateDueToNonStringValue() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> validator.ensureValid(TOPIC_MAPPINGS, new Object()));
        assertThat(ce.getMessage())
                .isEqualTo("Invalid value for configuration \"topic.mappings\": Must be a string");
    }
}
