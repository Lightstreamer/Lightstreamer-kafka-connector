
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.producers.converters;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

public class JsonMessageConverterTest {

    static Stream<String> jsonMessagesWithInvalidTopics() {
        return Stream.of("{}", "{\"topic\": \"\"}", "{\"topic\": null}");
    }

    @ParameterizedTest
    @MethodSource("jsonMessagesWithInvalidTopics")
    void shouldFailConversionDueToInvalidTopic(String jsonMessage) {
        JsonMessageConverter<?, ?> converter =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.STRING).converter();

        InvalidMessageException exception =
                assertThrows(InvalidMessageException.class, () -> converter.fromJson(jsonMessage));
        assertThat(exception)
                .hasMessageThat()
                .contains("Invalid \"topic\" field: expected a non-blank string");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"{", "}", "{\"topic\": \"a-topic\", "})
    void shouldFailConversionDueToMalformedJson(String jsonMessage) {
        JsonMessageConverter<?, ?> converter =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.STRING).converter();

        InvalidMessageException exception =
                assertThrows(InvalidMessageException.class, () -> converter.fromJson(jsonMessage));
        assertThat(exception).hasMessageThat().startsWith("Malformed JSON: ");
    }

    static Stream<String> jsonMessagesWithMissingKeyAndValue() {
        return Stream.of(
                "{\"topic\": \"test-topic\"}",
                "{\"topic\": \"test-topic\", \"key\": null, \"value\": null}",
                "{\"topic\": \"test-topic\", \"key\": null}",
                "{\"topic\": \"test-topic\", \"value\": null}");
    }

    @ParameterizedTest
    @MethodSource("jsonMessagesWithMissingKeyAndValue")
    void shouldFailConversionDueToMissingKeyAndValue(String jsonMessage) {
        JsonMessageConverter<?, ?> converter =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.STRING).converter();

        InvalidMessageException exception =
                assertThrows(InvalidMessageException.class, () -> converter.fromJson(jsonMessage));
        assertThat(exception)
                .hasMessageThat()
                .contains("Invalid message: both \"key\" and \"value\" are missing");
    }

    static Stream<Arguments> jsonMessagesWithInvalidKeyOrValue() {
        return Stream.of(
                Arguments.of(
                        "{\"topic\": \"test-topic\", \"key\": \"A string\"}",
                        "Invalid \"key\" field: expected INTEGER but found STRING"),
                Arguments.of(
                        "{\"topic\": \"test-topic\", \"value\": 123}",
                        "Invalid \"value\" field: expected STRING but found NUMBER"));
    }

    @ParameterizedTest
    @MethodSource("jsonMessagesWithInvalidKeyOrValue")
    void shouldFailConversionDueToInvalidKeyOrValue(String jsonMessage, String expectedMessage) {
        JsonMessageConverter<?, ?> converter =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.STRING).converter();

        InvalidMessageException exception =
                assertThrows(InvalidMessageException.class, () -> converter.fromJson(jsonMessage));
        assertThat(exception).hasMessageThat().isEqualTo(expectedMessage);
    }

    static Stream<Arguments> jsonMessages() {
        return Stream.of(
                Arguments.of("{\"topic\": \"test-topic\", \"key\": 123}", 123, null),
                Arguments.of("{\"topic\": \"test-topic\", \"value\": \"value\"}", null, "value"));
    }

    @ParameterizedTest
    @MethodSource("jsonMessages")
    void shouldCreateJsonMessageConverter(
            String jsonMessage, Object expectedKey, Object expectedValue)
            throws InvalidMessageException {
        JsonMessageConverter<?, ?> converter =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.STRING).converter();
        assertThat(converter).isNotNull();

        ProducerRecord<?, ?> record = converter.fromJson(jsonMessage);

        assertThat(record.topic()).isEqualTo("test-topic");
        assertThat(record.key()).isEqualTo(expectedKey);
        assertThat(record.value()).isEqualTo(expectedValue);
    }
}
