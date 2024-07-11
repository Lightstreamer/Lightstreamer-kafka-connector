
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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class TopicMappingsValidatorTest {

    TopicMappingsValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new TopicMappingsValidator();
    }

    static Stream<Arguments> wrongValues() {
        return Stream.of(
                // Null topic.mappings
                arguments(
                        null,
                        "Invalid value for configuration \"topic.mappings\": Must be non-null"),
                // Empty topic.mappings
                arguments(
                        Collections.emptyList(),
                        "Invalid value for configuration \"topic.mappings\": Must be a non-empty list"),
                // List of empty topic.mappings
                arguments(
                        List.of(""),
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings"),
                arguments(
                        List.of("", ""),
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings"),
                arguments(
                        List.of(" ", ""),
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings"),
                // List of mixed non-empty/empty-strings
                arguments(
                        List.of("item1", ""),
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings"));
    }

    @ParameterizedTest
    @MethodSource("wrongValues")
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> validator.ensureValid(TOPIC_MAPPINGS, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> values() {
        return Stream.of(arguments(List.of("item1")), arguments(List.of("item1", "item2")));
    }

    @ParameterizedTest
    @MethodSource("values")
    public void shouldValidate(Object value) {
        assertDoesNotThrow(() -> validator.ensureValid(TOPIC_MAPPINGS, value));
    }
}
