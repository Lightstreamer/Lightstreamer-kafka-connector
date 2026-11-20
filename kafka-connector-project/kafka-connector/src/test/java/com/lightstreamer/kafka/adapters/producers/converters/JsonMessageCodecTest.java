
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.BooleanPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.DoublePayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.FloatPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.IntegerPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.JsonPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.LongPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.ShortPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.StringPayloadConverter;
import com.lightstreamer.kafka.adapters.producers.converters.JsonPayloadConverters.UUIDPayloadConverter;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class JsonMessageCodecTest {

    static Stream<Arguments> supportedTypes() {
        return Stream.of(
                Arguments.of(PayloadConverterType.STRING, StringPayloadConverter.class),
                Arguments.of(PayloadConverterType.INTEGER, IntegerPayloadConverter.class),
                Arguments.of(PayloadConverterType.BOOLEAN, BooleanPayloadConverter.class),
                Arguments.of(PayloadConverterType.DOUBLE, DoublePayloadConverter.class),
                Arguments.of(PayloadConverterType.FLOAT, FloatPayloadConverter.class),
                Arguments.of(PayloadConverterType.LONG, LongPayloadConverter.class),
                Arguments.of(PayloadConverterType.SHORT, ShortPayloadConverter.class),
                Arguments.of(PayloadConverterType.UUID, UUIDPayloadConverter.class),
                Arguments.of(PayloadConverterType.JSON, JsonPayloadConverter.class));
    }

    @ParameterizedTest
    @MethodSource("supportedTypes")
    void shouldLookup(PayloadConverterType type, Class<?> expectedPayloadConverter) {
        PayloadConverter converter = JsonMessageCodec.lookup(type);
        assertThat(converter).isInstanceOf(expectedPayloadConverter);
    }

    @ParameterizedTest
    @EnumSource(
            value = PayloadConverterType.class,
            names = {"AVRO", "PROTOBUF", "BYTES", "BYTE_ARRAY", "BYTE_BUFFER"})
    void shouldFailDueToUnsupportedType(PayloadConverterType type) {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> JsonMessageCodec.lookup(type));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo("Unsupported payload converter type for publishing: " + type);
    }

    @Test
    void shouldFailDueToNullEvaluatorType() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> JsonMessageCodec.lookup(null));
        assertThat(e).hasMessageThat().isEqualTo("Payload converter type cannot be null");
    }
}
