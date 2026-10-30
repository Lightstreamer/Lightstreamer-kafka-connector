
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class JsonMessageCodecTest {

    static Stream<Arguments> types() {
        return Stream.of(
                Arguments.of(
                        EvaluatorType.STRING,
                        org.apache.kafka.common.serialization.StringSerializer.class,
                        EvaluatorType.JSON,
                        org.apache.kafka.common.serialization.StringSerializer.class),
                Arguments.of(
                        EvaluatorType.INTEGER,
                        org.apache.kafka.common.serialization.IntegerSerializer.class,
                        EvaluatorType.UUID,
                        org.apache.kafka.common.serialization.UUIDSerializer.class)
        );
    }

    @ParameterizedTest
    @MethodSource("types")
    void shouldCreateJsonMessageCodecForGivenEvaluatorTypes(
            EvaluatorType keyType,
            Class<?> expectedKeySerializer,
            EvaluatorType valueType,
            Class<?> expectedValueSerializer) {
        JsonMessageCodec<?, ?> codec = JsonMessageCodec.of(keyType, valueType);
        assertThat(codec.keySerializer()).isInstanceOf(expectedKeySerializer);
        assertThat(codec.valueSerializer()).isInstanceOf(expectedValueSerializer);
        
        JsonMessageConverter<?,?> converter = codec.converter();
        assertThat(converter).isNotNull();
    }
}
