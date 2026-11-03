
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;

import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class JsonMessageCodecTest {

    static Stream<Arguments> supportedTypes() {
        return Stream.of(
                Arguments.of(EvaluatorType.STRING, StringSerializer.class),
                Arguments.of(EvaluatorType.INTEGER, IntegerSerializer.class),
                Arguments.of(EvaluatorType.BOOLEAN, BooleanSerializer.class),
                Arguments.of(EvaluatorType.DOUBLE, DoubleSerializer.class),
                Arguments.of(EvaluatorType.FLOAT, FloatSerializer.class),
                Arguments.of(EvaluatorType.LONG, LongSerializer.class),
                Arguments.of(EvaluatorType.SHORT, ShortSerializer.class),
                Arguments.of(EvaluatorType.UUID, UUIDSerializer.class),
                Arguments.of(EvaluatorType.JSON, StringSerializer.class));
    }

    @ParameterizedTest
    @MethodSource("supportedTypes")
    void shouldCreateCodecForSupportedEvaluatorType(
            EvaluatorType type, Class<?> expectedSerializer) {
        JsonMessageCodec<?, ?> codec = JsonMessageCodec.of(type, type);
        assertThat(codec.keySerializer()).isInstanceOf(expectedSerializer);
        assertThat(codec.valueSerializer()).isInstanceOf(expectedSerializer);

        JsonMessageConverter<?, ?> converter = codec.converter();
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldCreateCodecWithDistinctKeyAndValueTypes() {
        JsonMessageCodec<?, ?> codec =
                JsonMessageCodec.of(EvaluatorType.INTEGER, EvaluatorType.JSON);
        assertThat(codec.keySerializer()).isInstanceOf(IntegerSerializer.class);
        assertThat(codec.valueSerializer()).isInstanceOf(StringSerializer.class);
    }

    @ParameterizedTest
    @EnumSource(
            value = EvaluatorType.class,
            names = {"AVRO", "PROTOBUF", "KVP", "BYTES", "BYTE_ARRAY", "BYTE_BUFFER"})
    void shouldFailDueToUnsupportedEvaluatorType(EvaluatorType type) {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> JsonMessageCodec.of(type, EvaluatorType.STRING));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo("Unsupported evaluator type for publishing: " + type);

        e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> JsonMessageCodec.of(EvaluatorType.STRING, type));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo("Unsupported evaluator type for publishing: " + type);
    }

    @Test
    void shouldFailDueToNullEvaluatorType() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> JsonMessageCodec.of(null, EvaluatorType.STRING));
        assertThat(e).hasMessageThat().isEqualTo("Unsupported evaluator type for publishing: null");

        e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> JsonMessageCodec.of(EvaluatorType.STRING, null));
        assertThat(e).hasMessageThat().isEqualTo("Unsupported evaluator type for publishing: null");
    }
}
